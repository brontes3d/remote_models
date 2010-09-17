require File.join(File.dirname(__FILE__), "test_helper")
require 'mocha'

require File.join(File.dirname(__FILE__), "..", "..", "amqp_listener", "init")

require 'activerecord'
require 'memcache'

require File.expand_path(File.dirname(__FILE__) + '/mocks/mock_app.rb')
require File.expand_path(File.dirname(__FILE__) + '/mocks/event_log.rb')
require File.expand_path(File.dirname(__FILE__) + '/mocks/mem_cache_mock.rb')
require File.expand_path(File.dirname(__FILE__) + '/mocks/messaging_system_mock.rb')
require File.expand_path(File.dirname(__FILE__) + '/mocks/provider/provider_app.rb')
require File.expand_path(File.dirname(__FILE__) + '/mocks/consumer/consumer_app.rb')

class RemoteModelsTest < Test::Unit::TestCase
  
  def setup
    super
    class << (AmqpListener.listeners)
      def <<(arg)
        #ignored
      end
    end    
  end
  
  def test_escape_ident
    assert_equal("B3MMM232023", RemoteModels.escape_ident("B3MMM232023"))
    assert_equal("B3MMM232023_", RemoteModels.escape_ident("B3MMM-232023_"))
    assert_equal("B3MMM232023", RemoteModels.escape_ident("?? B3MMM * 232023"))
    assert_equal("123abcxyz", RemoteModels.escape_ident("123abcxyz"))
    assert_raises(ArgumentError) do
      RemoteModels.escape_ident("1234abcxyz1234abcxyz1234abcxyz1234abcxyz1234abcxyz1")
    end
  end
  
  def test_to_avoid_extra_change_notifications
    ProviderApp.load_stuff
    AmqpListener.expects(:send_to_exchange).with do |routing_key, message|
      true
    end.once
    bean = Bean.create!(:name => "String", :color => "green", :sweetness => "medium")
    bean.save!
    bean.save!
  end
  
  def test_enable_change_notifications
    memcache_mock = MemCacheMock.new(Thread.current)
    messaging_system_mock = MessagingSystemMock.new(Thread.current)
    provider_app = ProviderApp.new
    consumer_app = ConsumerApp.new
    
    provider_app.will do
      Bean.create!(:name => "String", :color => "green", :sweetness => "medium")
    end
    consumer_app.will do
      AmqpListener.listeners[0] = BeanExposer
      AmqpListener.run
    end
    
    provider_thread = Thread.new do
      provider_app.run
    end
    provider_thread_group = ThreadGroup.new
    provider_thread_group.add(provider_thread)
    consumer_thread = Thread.new do
      sleep(0.2)
      consumer_app.run
    end
    consumer_thread_group = ThreadGroup.new
    consumer_thread_group.add(consumer_thread)
    
    event = messaging_system_mock.pop_event
    assert_equal(:exchange_bind, event.method)
    assert_equal({:durable=>true, :auto_delete=>false}, event.args.last)
    assert_equal("mr_bean_updates.*", event.args.first)
    assert_equal(consumer_thread_group, event.thread.group)
    event.resume!
    
    event = messaging_system_mock.pop_event
    assert_equal(:message_send, event.method)
    assert_equal({:key => "mr_bean_updates.live"}, event.args.last)
    assert_equal("amq.topic", event.args.first)
    assert_equal({'name' => "String", 'color' => "green", 'sweetness' => "medium", 'id' => 1}, 
                  ActiveSupport::JSON.decode(event.args[1]))
    assert_equal(provider_thread_group, event.thread.group)
    event.resume!

    event = messaging_system_mock.pop_event
    assert_equal(:message_handle, event.method)
    assert_equal({:key => "mr_bean_updates.live"}, event.args.last)
    assert_equal(["amq.topic", "mr_bean_updates.*"], event.args.first)
    assert_equal({'name' => "String", 'color' => "green", 'sweetness' => "medium", 'id' => 1}, 
                  ActiveSupport::JSON.decode(event.args[1]))
    assert_equal(consumer_thread_group, event.thread.group)
    event.resume!
    
    provider_thread.join
    (provider_thread_group.list + consumer_thread_group.list).each{ |th| th.kill! }
  end
  
  def test_provide_and_consume
    memcache_mock = MemCacheMock.new(Thread.current)
    messaging_system_mock = MessagingSystemMock.new(Thread.current)
    provider_app = ProviderApp.new
    consumer_app = ConsumerApp.new
    
    provider_app.will do
      Pepper.create!(:name => "Cascabel", :color => "red", :intensity => "mild")
    end
    provider_app.will do
      AmqpListener.listeners[0] = PepperExposer
      AmqpListener.run
    end
    
    consumer_app.will do
      remote_peppers = RemotePepper.find(:all)
      remote_peppers.fetch
      remote_peppers
    end
    
    provider_thread = Thread.new do
      provider_app.run
    end
    provider_thread_group = ThreadGroup.new
    provider_thread_group.add(provider_thread)
    consumer_thread = Thread.new do
      sleep(0.2)
      consumer_app.run
    end
    consumer_thread_group = ThreadGroup.new
    consumer_thread_group.add(consumer_thread)

    #provider should expose the get_q
    event = messaging_system_mock.pop_event
    assert_equal(:q_create, event.method)
    assert_equal({:durable=>false, :auto_delete=>false}, event.args.last)
    assert_equal(provider_thread_group, event.thread.group)
    event.resume!
    
    #then the consumer should look in memcahce, find nothing, then send a message to fetch it
    event = memcache_mock.pop_event
    assert_equal([:get, ['remote_models:sergeant_peppers_fetch_all']], event.method_and_args)
    event.resume!
        
    event = messaging_system_mock.pop_event
    assert_equal(:message_send, event.method)
    assert_equal(consumer_thread_group, event.thread.group)
    assert_equal('sergeant_peppers_get', event.args[0])
    message_got = event.args[1]
    args_decoded = ActiveSupport::JSON.decode(message_got)
    assert_equal("fetch_all", args_decoded["identifier"])
    response_q = args_decoded["response_q"]
    sent_with_opts = event.args[2]
    assert_equal(sent_with_opts, {:persistent => false})
    event.resume!
    
    event = messaging_system_mock.pop_event
    assert_equal(:q_create, event.method)
    assert_equal(response_q, event.args.first)
    assert_equal({:durable=>false, :auto_delete=>true}, event.args.last)
    assert_equal(consumer_thread_group, event.thread.group)
    event.resume!
    
    #then the provider should handle that message, put something into memcache, and send "ok" to the consumer
    event = messaging_system_mock.pop_event
    assert_equal(:message_handle, event.method)
    assert_equal(provider_thread_group, event.thread.group)
    assert_equal(['sergeant_peppers_get', message_got, {:persistent => false}], event.args)
    event.resume!
        
    event = memcache_mock.pop_event
    assert_equal(provider_thread_group, event.thread.group)
    assert_equal([:set, ['remote_models:sergeant_peppers_fetch_all', 
                         [{"Cascabel" => {:name => "Cascabel", :color => "red", :intensity => "mild"}}]]], 
                 event.method_and_args)
    event.resume!
        
    event = messaging_system_mock.pop_event
    assert_equal(:message_send, event.method)
    assert_equal(provider_thread_group, event.thread.group)
    assert_equal([response_q, "ok", {:persistent => false}], event.args)
    event.resume!
        
    #then the consumer should get the "ok" message and pull the thing from memcache
    event = messaging_system_mock.pop_event
    assert_equal(:message_handle, event.method)
    assert_equal(consumer_thread_group, event.thread.group)
    assert_equal([response_q, "ok", {:persistent=>false}], event.args)
    event.resume!
    
    
    event = memcache_mock.pop_event
    assert_equal([:get, ['remote_models:sergeant_peppers_fetch_all']], event.method_and_args)
    assert_equal(consumer_thread_group, event.thread.group)
    event.resume!
    
    assert_equal([], memcache_mock.events)
    assert_equal([], messaging_system_mock.events)
    
    consumer_thread.join
    
    cascabel = provider_app.results[0]
    remote_peppers = consumer_app.results[0]
        
    assert_equal cascabel.name, remote_peppers[0].name
    assert_equal cascabel.color, remote_peppers[0].color
    assert_equal cascabel.intensity, remote_peppers[0].intensity
    
    (provider_thread_group.list + consumer_thread_group.list).each{ |th| th.kill! }
  end
  
end
