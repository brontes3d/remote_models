class RemotePepper
  
  attr_accessor :name
  attr_accessor :color
  attr_accessor :intensity
  
  include RemoteModels::Consumer
  consumes_remote_model("sergeant_peppers", :identifier => 'name')
  
end

class RemoteBean
  
  cattr_accessor :messages_received
  
  attr_accessor :name
  attr_accessor :color
  attr_accessor :sweetness
  
  def self.bean_handler
    Proc.new do |atts|
      RemoteBean.messages_received ||= []
      RemoteBean.messages_received << atts
    end
  end
  
  include RemoteModels::Receiver
  receives_remote_model("mr_bean", &self.bean_handler)
  
end

class BeanExposer < AmqpListener::Listener
  RemoteModels.expose_receiver(self, RemoteBean)
end
