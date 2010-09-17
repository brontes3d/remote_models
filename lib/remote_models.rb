class RemoteModels
  FETCH_ALL = "fetch_all".freeze
  REQUEST_UPDATES_ALL = "request_updates_all".freeze
  
  def self.get_default_config
    {
      :memcache_config => (defined?(MEMCACHE_CONFIG) && MEMCACHE_CONFIG) || [], 
      :key_prefix => "remote_models:",
      :default_expire_time => 60
    }
  end
  
  def self.get_config
    @@config ||= get_default_config
  end
  
  def self.config(config_given)
    @@config = get_default_config.merge(config_given)
  end
  
  def self.populate_object_from_hash(object, hash)
    hash.each do |k, v|
      if object.respond_to?(k)
        object.send("#{k}=", v)
      end
    end
    object
  end
  
  def self.escape_ident(identifier)
    to_return = identifier.to_s.gsub(/[^a-zA-Z0-9_]/,'')
    if to_return.size > 50
      raise ArgumentError, "identifier #{identifier} is too long for use with remote models"
    end
    to_return
  end
  
  def self.memcache_storage_key(remote_model_name, identifier)
    to_return = get_config[:key_prefix] + "#{remote_model_name}_#{escape_ident(identifier)}"
    # puts "storage key #{to_return}"
    to_return
  end
  
  def self.name_of_get_q(remote_model_name)
    "#{remote_model_name}_get"
  end
  
  def self.memcache_connection
	  require 'memcache'
    #TODO: make this more configurable (in a better way)
    #TODO: save and re-use connection to memcache?
    MemCache.new(*get_config[:memcache_config])
  end
  
  def self.default_expire_time
    get_config[:default_expire_time]
  end

  def self.fetch_together!(*models, &block)
    fetch_together_bang(models, true, &block)
  end

  def self.fetch_together(*models, &block)
    fetch_together_bang(models, false, &block)
  end
  
  def self.fetch_together_bang(models, bang, &block)
    models_needing_fetch = []
    models.each do |model|
      unless model.fetched
    	  unless model.read_from_memcached
	        models_needing_fetch << model
        end
      end
    end
    if models_needing_fetch.empty?
      if block_given?
        yield
      end
    else
      self.do_fetch(models_needing_fetch, {:bang => bang}, &block)
    end
  end
  
  def self.request_updates(*models, &block)
    tasks = []
    models.each do |model|
      AmqpListener.send(RemoteModels.name_of_get_q(model.receives_remote_model_named), 
        {:identifier => REQUEST_UPDATES_ALL}.to_json, false)
    end
  end
  
  class FetchTimeout < StandardError; end    
  
  def self.do_fetch(models, options = {}, &block)
    force = options[:bang]
    timeout = (options[:timeout] || AmqpListener.running_config[:task_timeout] || 5).to_i
    waiting = []
    models.each do |model|
      response_q = "#{model.consume_remote_model_named}_response" + UUID.generate
      identifier_value = model.send(model.consume_remote_model_identifier)
      send_q_name = RemoteModels.name_of_get_q(model.consume_remote_model_named)
      message = {:response_q => response_q, :identifier => identifier_value}.to_json
      task_info = {:model => model, :message => message}
      waiting << task_info
      Thread.new do   
        AmqpListener.with_bunny do |bunny|
          queue = bunny.queue(response_q, :auto_delete => true, :durable => false)
          AmqpListener.send(send_q_name, message, false, {}, {}, bunny)
          queue.subscribe(:message_max => 1, :timeout => timeout) do |msg|
            puts "got: " + msg.inspect
            model.read_from_memcached
            waiting.delete(task_info)
          end
        end
      end
    end
    begin
      Timeout::timeout(timeout) do
        while(!waiting.empty?) do
          Thread.pass
        end
      end
    rescue Timeout::Error => e
      if force
        raise FetchTimeout, "Timeout running tasks #{waiting.inspect}"
      end
    end
    if block_given?
      yield
    end
  end
    
  class RemoteModel
    attr_accessor :name, :local_model, :identifier, :expire_time
    def initialize(name, local_model, opts)
      self.name = name
      self.local_model = local_model
      self.identifier = opts[:identifier] || "id"
      self.expire_time = (opts[:expire_time] || RemoteModels.default_expire_time).to_i
      self.find_by_param_blocks = {}
    end
    attr_accessor :formatting_blocks
    def format(format_name = "default", &block)
      self.formatting_blocks ||= {}
      self.formatting_blocks[format_name] = block
    end
    def formatting_block(format_name)
      self.formatting_blocks[format_name]
    end
    attr_accessor :find_block
    def find(&block)
      self.find_block = block
    end
    attr_accessor :find_all_block
    def find_all(&block)
      self.find_all_block = block
    end
    attr_accessor :find_by_param_blocks
    def find_all_by_param(name, &block)
      self.find_by_param_blocks[name.to_s] = block      
    end
    def enable_change_notifications(opts = {})
      routing_key = opts[:routing_key] || "#{self.name}_updates"
      format = opts[:format] || "default"
      # push_method_name = RemoteModels.name_of_push_method(self.name, q)
      local_model.class_eval do
        def push_remote_models_change_notification(live = true)
          self.class.remote_models_change_notifications.each do |pusher|
            pusher.call(self, live)
          end
          true
        end
        cattr_accessor :remote_models_change_notifications
      end
      local_model.after_save :push_remote_models_change_notification, :if => Proc.new{ |obj| obj.changed? }
      local_model.remote_models_change_notifications ||= []
      local_model.remote_models_change_notifications << Proc.new do |model, live|
        data_to_send = self.make_attributes_hash_for_memchaced(model, format).to_json
        if live
          AmqpListener.send_to_exchange("#{routing_key}.live", data_to_send)
        else
          AmqpListener.send_to_exchange("#{routing_key}.requested", data_to_send)
        end
      end
    end
    def push_list_to_memcached(local_list, storage_key_suffix = FETCH_ALL)
      RemoteModels.memcache_connection.set(
                        RemoteModels.memcache_storage_key(self.name, storage_key_suffix), 
                        self.make_list_for_memchaced(local_list), 
                        self.expire_time)
    end
    def make_list_for_memchaced(local_list)
      to_return = []
      local_list.each do |obj|
        to_return << { obj.send(self.identifier) => make_attributes_hash_for_memchaced(obj) }
      end
      to_return
    end
    def push_to_memchaced(local_object)
      RemoteModels.memcache_connection.set(
                        RemoteModels.memcache_storage_key(self.name, local_object.send(identifier)), 
                        self.make_attributes_hash_for_memchaced(local_object), 
                        self.expire_time)
    end
    def make_attributes_hash_for_memchaced(local_object, format = 'default')
      if format_block = self.formatting_block(format)
        format_block.call(local_object)
      else
        local_object.attributes
      end
    end
  end
  
  module Receiver
    module ClassMethods
      def receives_remote_model(name, opts = {}, &block)
        self.receives_remote_model_named = name
        self.receives_remote_model_handler = block
        self.receives_remote_model_bind_key = opts[:bind_key] || "#{name}_updates.*"
      end
      def request_updates
        RemoteModels.request_updates(self)
      end
    end
    def self.included(base)
      base.class_eval do
        base.extend ClassMethods
        cattr_accessor :receives_remote_model_named
        cattr_accessor :receives_remote_model_handler
        cattr_accessor :receives_remote_model_bind_key
        def receives_remote_model_named
          self.class.receives_remote_model_named
        end
        def receives_remote_model_bind_key
          self.class.receives_remote_model_bind_key
        end
      end
    end
  end
  
  module Consumer
    module FetchAndReadMethods
      def self.included(base)
        base.class_eval do
          cattr_accessor :consume_remote_model_named
          cattr_accessor :consume_remote_model_identifier
          attr_accessor :fetched, :failed_fetch
        end
      end
      def get_memcached_key
        RemoteModels.memcache_storage_key(
            self.consume_remote_model_named,
            self.send(self.consume_remote_model_identifier))        
      end
      def read_from_memcached
        memcached_key = get_memcached_key
        # puts "read from #{memcached_key}"
        if fetched_value = RemoteModels.memcache_connection.get(memcached_key)
          if self.is_a?(RemoteArray)
            #TODO: assert fetched_value is an array of hashes
            fetched_value.each do |hash|
              ident = hash.keys[0]
              to_add = self.local_model_class.new
              to_add.send("#{self.local_model_identifier}=", ident)
              RemoteModels.populate_object_from_hash(to_add, hash[ident])
              # hash[ident].each do |k, v|
              #   if to_add.respond_to?(k)
              #     to_add.send("#{k}=", v)
              #   end                
              # end
              self << to_add
            end
          else
            #TODO: assert fetched_value is a Hash
            hash_value = fetched_value
            RemoteModels.populate_object_from_hash(self, hash_value)
            # hash_value.each do |k, v|
            #   if self.respond_to?(k)
            #     self.send("#{k}=", v)
            #   end
            # end
          end
          self.fetched = true
          true
        else
          false
        end
      end
      def fetch!(options = {}, &block)
        self.fetch(options.merge(:bang => true), &block)
    	end
    	def fetch(options = {}, &block)
    	  require 'uuid'
    	  @block_called = false
    	  unless fetched
    	    unless failed_fetch
        	  unless read_from_memcached
        	    RemoteModels.do_fetch([self], options, &block)
        	    @block_called = true
            end
          end
        end
        if block_given? && !@block_called
          yield
        end
        fetched
      end    	
    end
    class RemoteArray < Array
      include FetchAndReadMethods
      attr_reader :consume_remote_model_named, :consume_remote_model_identifier
      attr_reader :local_model_class, :local_model_identifier
      attr_reader :fetch_params
      def initialize(model_named, model_identifier, local_model_class, local_model_identifier, fetch_params)
        @consume_remote_model_named = model_named
        @consume_remote_model_identifier = model_identifier
        @local_model_class = local_model_class
        @local_model_identifier = local_model_identifier
        @fetch_params = fetch_params
      end
      def all
        if @fetch_params
          @fetch_params
        else
          FETCH_ALL
        end
      end
      def each(&block)
        fetch
        super
      end
    end
    module ClassMethods
      def consumes_remote_model(name, opts = {})
        self.consume_remote_model_named = name
        self.consume_remote_model_identifier = opts[:identifier] || "#{name}_id"
      end
      def find(identifier_value, options = {})
        params = options[:params]
        if identifier_value == :all
          RemoteArray.new(self.consume_remote_model_named, identifier_value, 
                          self, self.consume_remote_model_identifier, params)
        else
          to_return = self.new
          to_return.send("#{self.consume_remote_model_identifier}=", identifier_value)
          to_return.fetched = false
          to_return
        end
      end
    end
    def self.included(base)
      base.extend ClassMethods
      base.class_eval do
        include FetchAndReadMethods
        def consume_remote_model_named
          self.class.consume_remote_model_named
        end
        def consume_remote_model_identifier
          self.class.consume_remote_model_identifier
        end        
      end
    end
  end
  
  module Provider
    module ClassMethods
      def provides_remote_model(name, opts = {}, &block)
         created_model = RemoteModel.new(name, self, opts)
         created_model.instance_eval(&block)
         self.remote_models[name] = created_model
      end
      def remote_model(named)
        self.remote_models[named]
      end
    end
    
    def self.included(base)
      base.class_eval do
        cattr_accessor :remote_models        
      end
      base.remote_models ||= {}
      base.extend ClassMethods
    end
  end
  
  def self.expose_receiver(listener, receiver_model, message_handler_method = :on_message)
    #Q name doesn't really need to contain all this information
    #but we need to make sure it's unique per-app, per-listener class, and adding on the bind key is convenient for debugging
    q_name = RAILS_ROOT.split("/").last + ":" + listener.name + ":" + receiver_model.receives_remote_model_bind_key
    listener.subscribes_to q_name
    listener.queue_options :durable => true, :auto_delete => false
    listener.message_format :json_hash
    listener.exchange_config :bind_key => receiver_model.receives_remote_model_bind_key
    listener.send(:define_method, message_handler_method) do |options|
      receiver_model.receives_remote_model_handler.call(options)
    end
  end
    
  def self.expose_provider(listener, provider, message_handler_method = :on_message)
    listener.subscribes_to RemoteModels.name_of_get_q(provider.name)
    listener.queue_options :durable => false, :auto_delete => false
    listener.message_format :json_hash
    listener.send(:define_method, message_handler_method) do |options|
      identifier_key = provider.identifier
      options.symbolize_keys!
      identifier = options[:identifier]
      if identifier == REQUEST_UPDATES_ALL
        provider.local_model.find_in_batches(:batch_size => 250) do |batch_of_local_objects|
          batch_of_local_objects.each do |local_object|
            #call the normal after_save method as if object was just changed
            #false means this is not a live update, so routing key will be remote_model_name.requested instead of remote_model_name.live
            local_object.push_remote_models_change_notification(false)
          end
        end
      else
        response_q = options[:response_q]
        if identifier == FETCH_ALL
          local_list = (provider.find_all_block || 
                        Proc.new{ provider.local_model.find(:all) }).call
          provider.push_list_to_memcached(local_list)
        elsif identifier.is_a?(Hash)
          key = identifier.keys.first
          value = identifier[key]
          local_list = provider.find_by_param_blocks[key.to_s].call(value)
          provider.push_list_to_memcached(local_list, identifier)
        else
          if local_object = (provider.find_block || 
                          (Proc.new do |i| 
                            provider.local_model.send("find_by_#{identifier_key}", i)
                          end)).call(identifier)
          then
            provider.push_to_memchaced(local_object)
          else
            raise ActiveRecord::RecordNotFound, "Couldn't find a #{provider.local_model} with identifier #{identifier}"
          end
        end
        AmqpListener.send(response_q, "ok", false, :auto_delete => true)
      end
    end
  end
  
end