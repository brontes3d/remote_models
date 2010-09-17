class ConsumerApp < MockApp
  
  def start
    require File.expand_path(File.dirname(__FILE__) + '/models.rb')
    
    self
  end
  
end