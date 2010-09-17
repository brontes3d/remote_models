class MemCacheMock < EventLog
  
  def store_contents
    @@store ||= {}
  end
  
  def get(key)
    @@store ||= {}
    record_event(:get, [key])
    @@store[key]
  end
  def set(key, value, expiry = nil)
    @@store ||= {}
    record_event(:set, [key, value])
    @@store[key] = value
  end
  def delete(key, expiry = 0)
    @@store ||= {}
    @@store.delete(key)
  end
  
  def initialize(main_thread)
    super
    MemCache.stubs(:new).returns(self)
  end
  
end