class ProviderApp < MockApp
  
  def self.load_stuff
    require 'tempfile'
    @tempfile = Tempfile.new("test.db")
    ActiveRecord::Base.establish_connection(:adapter => "sqlite3", :database => @tempfile.path)

    load File.expand_path(File.dirname(__FILE__) + "/schema.rb")
    require File.expand_path(File.dirname(__FILE__) + '/models.rb')    
  end
  
  def start
    ProviderApp.load_stuff    
    self
  end
  
end