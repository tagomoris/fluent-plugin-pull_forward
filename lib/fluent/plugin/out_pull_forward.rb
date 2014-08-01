require 'fluent/mixin/config_placeholders'
require 'fluent/mixin/certificate'
require 'webrick'
require 'webrick/https'

require_relative 'webrick_logger_bridge'

module Fluent
  class PullForwardOutput < BufferedOutput
    DEFAULT_PULLFORWARD_LISTEN_PORT = 24280

    Fluent::Plugin.register_output('pull_forward', self)

    config_param :self_hostname, :string
    include Fluent::Mixin::ConfigPlaceholders

    config_param :bind, :string, :default => '0.0.0.0'
    config_param :port, :integer, :default => DEFAULT_PULLFORWARD_LISTEN_PORT

    config_param :server_loglevel, :string, :default => 'WARN'
    config_param :auth_loglevel, :string, :default => 'FATAL'

    config_set_default :buffer_type, 'pullpool'
    config_set_default :flush_interval, 3600 # 1h

    # REQUIRED: buffer_path

    # same with TimeSlicedOutput + FileBuffer
    # 16MB * 256 -> 4096MB
    config_set_default :buffer_chunk_limit, 1024 * 1024 * 16 # 16MB
    config_set_default :buffer_queue_limit, 256

    include Fluent::Mixin::Certificate
    # REQUIRED: self_hostname
    # REQUIRED: 'cert_auto_generate yes' or 'cert_file_path PATH'

    config_section :user, param_name: :users do
      config_param :username, :string
      config_param :password, :string
    end

    def initialize
      super
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

      logger = $log
      auth_logger = Fluent::PluginLogger.new(logger)
      auth_logger.level = @auth_loglevel
      server_logger = Fluent::PluginLogger.new(logger)
      server_logger.level = @server_loglevel

      auth_db = HtpasswdDummy.new
      @users.each do |user|
        auth_db.set_passwd(realm, user.username, user.password)
      end
      authenticator = WEBrick::HTTPAuth::BasicAuth.new(
        :UserDB => auth_db,
        :Realm => realm,
        :Logger => Fluent::PullForward::WEBrickLogger.new(auth_logger),
      )

      @server = WEBrick::HTTPServer.new(
        :BindAddress => @bind,
        :Port => @port,
        # :DocumentRoot => '.',
        :Logger => Fluent::PullForward::WEBrickLogger.new(server_logger),
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
