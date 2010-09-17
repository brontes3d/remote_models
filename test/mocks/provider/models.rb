class Pepper < ActiveRecord::Base
  
  include RemoteModels::Provider  
  provides_remote_model("sergeant_peppers", :identifier => 'name') do
    format do |pepper|
      {
        :name => pepper.name,
        :color => pepper.color,
        :intensity => pepper.intensity
      }
    end
  end
  
end

class Bean < ActiveRecord::Base
  
  include RemoteModels::Provider  
  provides_remote_model("mr_bean", :identifier => 'name') do
    format do |bean|
      bean.attributes
    end
    enable_change_notifications
  end
  
end

class PepperExposer < AmqpListener::Listener
  RemoteModels.expose_provider(self, Pepper.remote_model('sergeant_peppers'))
end
