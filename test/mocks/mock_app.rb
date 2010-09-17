class MockApp
  
  def initialize
    @will_blocks = []
  end
  
  def will(&block)
    @will_blocks << block
  end
  
  attr_accessor :results
  
  def run
    @results = []
    begin
      self.start
      @will_blocks.each do |block|
        @results << block.call
      end
    rescue => e
      puts e.inspect
      puts e.backtrace.join("\n")
    end
  end
  
end