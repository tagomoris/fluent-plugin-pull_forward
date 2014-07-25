require 'fluent/mixin/config_placeholders'

module Fluent
  class PullForwardOutput < BufferedOutput
    DEFAULT_PULLFORWARD_LISTEN_PORT = 24280

    Fluent::Plugin.register_output('pullforward', self)

    config_param :self_hostname, :string
    include Fluent::Mixin::ConfigPlaceholders

    config_param :bind, :string, :default => '0.0.0.0'
    config_param :port, :integer, :default => DEFAULT_PULLFORWARD_LISTEN_PORT

    config_set_default :buffer_type, 'pullpool'
    config_set_default :flush_interval, 3600

    # REQUIRED: buffer_path

    # same with TimeSlicedOutput + FileBuffer
    # 256MB * 256 -> 64GB
    config_set_default :buffer_chunk_limit, 1024 * 1024 * 256 # 256MB
    config_set_default :buffer_queue_limit, 256

    config_param :cert_auto_generate, :bool, :default => false
    config_param :generate_private_key_length, :integer, :default => 2048

    config_param :generate_cert_country, :string, :default => 'US'
    config_param :generate_cert_state, :string, :default => 'CA'
    config_param :generate_cert_locality, :string, :default => 'Mountain View'
    config_param :generate_cert_common_name, :string, :default => nil

    config_param :cert_file_path, :string, :default => nil
    config_param :private_key_file, :string, :default => nil
    config_param :private_key_passphrase, :string, :default => nil

    config_section :user, param_name: :users do
      config_param :username, :string
      config_param :password, :string
    end

    def initialize
      super
      require 'socket'
      require 'webrick'
      require 'webrick/https'
    end

    unless method_defined?(:log)
      define_method("log") { $log }
    end

    def configure(conf)
      super
      if @users.size < 1
        raise Fluent::ConfigError, "no <user> sections specified"
      end
    end

    def start
      super
      @thread = Thread.new(&method(:run))
    end

    def shutdown
      @server.stop if @server
      @thread.kill
      @thread.join
    end

    def certificate
      return @cert, @key if @cert && @key

      if ! @cert_auto_generate and ! @cert_file_path
        raise Fluent::ConfigError, "Both of cert_auto_generate and cert_file_path are not specified. See README."
      end

      if @cert_auto_generate
        @generate_cert_common_name ||= @self_hostname

        key = OpenSSL::PKey::RSA.generate(@generate_private_key_length)

        digest = OpenSSL::Digest::SHA1.new
        issuer = subject = OpenSSL::X509::Name.new
        subject.add_entry('C', @generate_cert_country)
        subject.add_entry('ST', @generate_cert_state)
        subject.add_entry('L', @generate_cert_locality)
        subject.add_entry('CN', @generate_cert_common_name)

        cer = OpenSSL::X509::Certificate.new
        cer.not_before = Time.at(0)
        cer.not_after = Time.at(0)
        cer.public_key = key
        cer.serial = 1
        cer.issuer = issuer
        cer.subject  = subject
        cer.sign(key, digest)

        @cert = cer
        @key = key
        return @cert, @key
      end

      @cert = OpenSSL::X509::Certificate.new(File.read(@cert_file_path))
      @key = OpenSSL::PKey::RSA.new(File.read(@private_key_file), @private_key_passphrase)
    end

    class HtpasswdDummy < WEBrick::HTTPAuth::Htpasswd
      # overwrite constructor NOT to generate htpasswd file on local filesystem
      def initialize
        @path = '/'
        @mtime = Time.at(0)
        @passwd = Hash.new
        @auth_type = WEBrick::HTTPAuth::BasicAuth
      end
    end

    def run
      cert, key = self.certificate
      realm = "Fluentd fluent-plugin-pullforward server"

      auth_db = HtpasswdDummy.new
      @users.each do |user|
        auth_db.set_passwd(realm, user.username, user.password)
      end
      authenticator = WEBrick::HTTPAuth::BasicAuth.new(
        :UserDB => auth_db,
        :Realm => realm,
        :Logger => WEBrick::Log.new(nil, WEBrick::BasicLog::FATAL),
      )

      @server = WEBrick::HTTPServer.new(
        :BindAddress => @bind,
        :Port => @port,
        # :DocumentRoot => '.',
        :Logger => WEBrick::Log.new(nil, WEBrick::BasicLog::WARN),
        :AccessLog => [],
        :SSLEnable  => true,
        :SSLCertificate => cert,
        :SSLPrivateKey => key
      )
      @server.logger.info("hogepos")

      @server.mount_proc('/') do |req, res|
        unless req.ssl?
          raise WEBrick::HTTPStatus::Forbidden, "pullforward plugin does not permit non-HTTPS requests"
        end
        if req.path != '/'
          raise WEBrick::HTTPStatus::NotFound, "valid path is only '/'"
        end
        authenticator.authenticate(req, res)
        res.content_type = 'application/json'
        res.body = dequeue_chunks()
      end

      @server.start
    end

    def format(tag, time, record)
      [tag, time, record].to_msgpack
    end

    def dequeue_chunks
      response = []

      unpacker = MessagePack::Unpacker.new

      @buffer.pull_chunks do |chunk|
        next if chunk.empty?
        unpacker.feed_each(chunk.read) do |ary|
          response << ary
        end
      end

      response.to_json
    end
  end
end
