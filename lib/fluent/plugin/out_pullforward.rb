require 'fluent/mixin/config_placeholders'
require 'fluent/mixin/certificate'

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

    include Fluent::Mixin::Certificate

    config_section :user, param_name: :users do
      config_param :username, :string
      config_param :password, :string
    end

    def initialize
      super
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

      log.info "listening pullforward socket on #{@bind}:#{@port}"
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
