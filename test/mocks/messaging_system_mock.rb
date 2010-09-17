class MessagingSystemMock < EventLog
  
  class QStub
    def initialize(self_message_mock, subscribe_callback, exchange_subscribe_callback, publish_callback, exchange_publish_callback)
      @self_message_mock
      @subscribe_callback = subscribe_callback
      @publish_callback = publish_callback
      @exchange_subscribe_callback = exchange_subscribe_callback
      @exchange_publish_callback = exchange_publish_callback
    end
    def subscribe(*args, &block)
      if @bound_to_exchange
        @exchange_subscribe_callback.call(@bound_to_exchange, @bound_to_exchange_with_opts, args, block)
      else
        @subscribe_callback.call(args, block)
      end
    end
    def unsubscribe
      true
    end
    def publish(message, message_opts)
      if @bound_to_exchange
        @exchange_publish_callback.call(@bound_to_exchange, @bound_to_exchange_with_opts, message, message_opts)        
      else
        @publish_callback.call(message, message_opts)
      end
    end
    def bind(exchange, opts)
      @bound_to_exchange = exchange
      @bound_to_exchange_with_opts = opts
    end
  end
  class HeaderStub
    def ack
      true
    end
  end
  
  def initialize(main_thread)
    super
    message_handlers = {}
    self_message_mock = self
    q_publish_block = Proc.new do |message, message_opts|
      record_event(:message_send, [arg1, arg2])
    end
    q_method = Proc.new do |q_name, q_opts|
      # puts "!! creating Q #{q_name} - #{q_opts}"
      # self_message_mock.record_event(:q_create, [q_name, q_opts])   
      from_thread = Thread.current
      QStub.new(
        self_message_mock, 
        (Proc.new do |args, block|
          # puts "!! creating Q #{q_name} - #{q_opts}"
          self_message_mock.record_event(:q_create, [q_name, q_opts])
          if message_handlers[q_name]
            raise "Already have a message handler on Q: #{q_name}"
          end
          message_handlers[q_name] = [from_thread, block]
        end),
        (Proc.new do |exchange, opts, args, block|
          # puts "exchange subscribe #{exchange.inspect} -- #{opts.inspect} -- #{args.inspect}"
          # puts "!! creating Q #{q_name} - #{q_opts}"
          exchange_key = opts[:key]
          self_message_mock.record_event(:exchange_bind, [exchange_key, q_opts])
          message_handlers["AMQTOPIC"] ||= []
          message_handlers["AMQTOPIC"] << [exchange_key, from_thread, block]
        end),
        (Proc.new do |message, message_opts|
          # puts "!! message_send #{[q_name, message, message_opts].inspect}"
          self_message_mock.record_event(:message_send, [q_name, message, message_opts])
          if message_opts[:key] && message_handlers["AMQTOPIC"]
            message_key = message_opts[:key]
            message_handlers["AMQTOPIC"].each do |info|
              (exchange_key, thread, handler) = info
              #FIXME! this is not a perfect == test... it needs to be more complicated than this if we test more complex things
              #example is comparing "mr_bean_updates.*" to "mr_bean_updates.live"
              # puts "TODO: compare #{exchange_key} with #{message_key}"
              if exchange_key.split(".").first == message_key.split(".").first     
                thread[:work] ||= []
                thread[:work] << [handler, [q_name, exchange_key], message, message_opts]
              end
            end
          elsif message_handlers[q_name]
            (thread, handler) = message_handlers[q_name]
            thread[:work] ||= []
            thread[:work] << [handler, q_name, message, message_opts]
          end
        end),
        (Proc.new do |args, block|
          raise "TODO: test mocks need to handle exchange publish"
        end)
      )
    end
    class << MQ; self; end.send(:define_method, :queue, &q_method)
    EM.stubs(:add_timer).returns(true)

    mq_stub = Object.new
    mq_stub.stubs(:exchanges => {})
    MQ.stubs(:new).returns(mq_stub)
    # AMQP.stubs(:start).returns(true)
    
    call_on_start_expectation = Proc.new do |expectation|
      class << expectation
        attr_accessor :capture_block
        def invoke(&block)
          result = super
          if @capture_block
            @capture_block.call(block)
          end
          result
        end
      end
      expectation.capture_block = Proc.new do |block|
        threads_to_check = []
        threads_to_check << Thread.current
        @msg_thread = Thread.new do
          begin
            threads_to_check << Thread.current
            block.call({}) if block
            threads_to_check.each{ |th| th[:work] ||= [] }
            while(true) do
              if thread_with_work = threads_to_check.detect{ |th| !th[:work].empty? }
                work = thread_with_work[:work].pop
                (handler, arg1, arg2, arg3) = work
                # puts "!! message_handle #{[arg1, arg2, arg3].inspect}"
                record_event(:message_handle, [arg1, arg2, arg3])
                if handler.arity == 1
                  handler.call(arg2)
                else
                  handler.call(HeaderStub.new, arg2)
                end
              else
                Thread.pass
                sleep(0.1)
              end        
            end          
          rescue => e
            puts e.inspect
            puts e.backtrace.join("\n")
          end
        end
      end   
    end
    
    call_on_start_expectation.call(AmqpListener.stubs(:start).returns(true))
    
    bunny_stub = Object.new
    class << bunny_stub; self; end.send(:define_method, :queue, &q_method)    
    class << bunny_stub; self; end.send(:define_method, :exchange, &q_method)
    Bunny.stubs(:new).returns(bunny_stub)
    
    call_on_start_expectation.call(bunny_stub.stubs(:start).returns(true))    
  end
  
end