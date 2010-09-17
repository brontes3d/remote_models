ActiveRecord::Schema.define do

  create_table "peppers", :force => true do |t|
    t.string   "name"
    t.string   "color"
    t.string   "intensity"
  end

  create_table "beans", :force => true do |t|
    t.string   "name"
    t.string   "color"
    t.string   "sweetness"
  end
  
end