class EventLog
  
  def events
    @events ||= []
  end
  
  class Event
    attr_reader :method, :args, :thread
    def initialize(method, args, thread)
      @method = method
      @args = args
      @thread = thread
    end
    def method_and_args
      [@method, @args]
    end
    def resume!
      @thread.run
    end
  end
  
  def pop_event
    while(!@events || @events.empty?) do
      Thread.pass
      sleep(0.1)
    end
    event_info = @events.pop
    Event.new(*event_info)
  end
  
  def record_event(method, args)
    @events ||= []
    unless Thread.current == @main_thread
      @events.unshift( [method, args, Thread.current] )      
      # puts "#{self} event happened #{method} -- #{args} in #{Thread.current}"
      Thread.stop
    end
  end
  
  def record_event_passive(method, args)
    @events ||= []
    @events.unshift( [method, args, Thread.current] )
  end
  
  def initialize(main_thread)
    @main_thread = main_thread    
  end
  
end