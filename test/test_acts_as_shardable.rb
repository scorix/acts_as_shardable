require 'minitest/autorun'
require 'active_record'
require 'acts_as_shardable'
require 'sqlite3'

class ActsAsShardableTest < Minitest::Test
  def setup
    ActiveRecord::Base.establish_connection(
        :adapter => 'sqlite3',
        :database => 'tmp/test.db'
    )

    ActiveRecord::Base.connection.tables.each do |table|
      ActiveRecord::Base.connection.drop_table(table)
    end

    schema = ActiveRecord::Schema.new
    schema.verbose = false
    schema.define(:version => 1) do
      2.times do |i|
        create_table('mod2_models_%04d' % i) do |t|
          t.integer :hash_id
          t.timestamps null: true
        end
      end
      4.times do |i|
        create_table('mod4_models_%04d' % i) do |t|
          t.integer :hash_id
          t.integer :lock_version
          t.timestamps null: true
        end
      end
    end
  end

  def test_mod_2_sharding
    assert_equal('mod2_models_0000', Mod2Model.sharding(0).table_name)
    assert_equal('mod2_models_0001', Mod2Model.sharding(1).table_name)
    assert_equal('mod2_models_0000', Mod2Model.sharding(2).table_name)
    assert_equal('mod2_models_0001', Mod2Model.sharding(3).table_name)
  end

  def test_mod_4_sharding
    assert_equal('mod4_models_0000', Mod4Model.sharding(0).table_name)
    assert_equal('mod4_models_0001', Mod4Model.sharding(1).table_name)
    assert_equal('mod4_models_0002', Mod4Model.sharding(2).table_name)
    assert_equal('mod4_models_0003', Mod4Model.sharding(3).table_name)
    assert_equal('mod4_models_0000', Mod4Model.sharding(4).table_name)
    assert_equal('mod4_models_0001', Mod4Model.sharding(5).table_name)
    assert_equal('mod4_models_0002', Mod4Model.sharding(6).table_name)
    assert_equal('mod4_models_0003', Mod4Model.sharding(7).table_name)
  end

  def test_mod2_class
    assert_equal('Mod2Model_0000', Mod2Model.sharding(0).name)
    assert_equal('Mod2Model_0001', Mod2Model.sharding(1).name)
    assert_equal('Mod2Model_0000', Mod2Model.sharding(2).name)
    assert_equal('Mod2Model_0001', Mod2Model.sharding(3).name)
  end

  def test_mod4_class
    assert_equal('Mod4Model_0000', Mod4Model.sharding(0).name)
    assert_equal('Mod4Model_0001', Mod4Model.sharding(1).name)
    assert_equal('Mod4Model_0002', Mod4Model.sharding(2).name)
    assert_equal('Mod4Model_0003', Mod4Model.sharding(3).name)
    assert_equal('Mod4Model_0000', Mod4Model.sharding(4).name)
    assert_equal('Mod4Model_0001', Mod4Model.sharding(5).name)
    assert_equal('Mod4Model_0002', Mod4Model.sharding(6).name)
    assert_equal('Mod4Model_0003', Mod4Model.sharding(7).name)
  end

  def test_save
    m = Mod2Model.new(hash_id: 0)
    assert(m.save)
    assert_equal(0, Mod2Model.sharding(1).count)
    assert_equal(1, Mod2Model.sharding(0).count)
    m = Mod2Model.new(hash_id: 1)
    assert(m.save)
    assert_equal(1, Mod2Model.sharding(1).count)
    assert_equal(1, Mod2Model.sharding(0).count)
  end

  def test_sharding_save
    m = Mod4Model.sharding(0).new(hash_id: 0)
    assert(m.save)
    assert_equal(0, Mod4Model.sharding(1).count)
    assert_equal(1, Mod4Model.sharding(0).count)
    m = Mod4Model.sharding(1).new(hash_id: 1)
    assert(m.save)
    assert_equal(1, Mod4Model.sharding(1).count)
    assert_equal(1, Mod4Model.sharding(0).count)
  end

  def test_create
    assert(Mod4Model.create(hash_id: 0))
    assert_equal(0, Mod4Model.sharding(1).count)
    assert_equal(1, Mod4Model.sharding(0).count)
    assert(Mod4Model.create(hash_id: 1))
    assert_equal(1, Mod4Model.sharding(1).count)
    assert_equal(1, Mod4Model.sharding(0).count)
  end

  def test_sharding_create
    assert(Mod4Model.sharding(0).create(hash_id: 0))
    assert_equal(0, Mod4Model.sharding(1).count)
    assert_equal(1, Mod4Model.sharding(0).count)
    assert(Mod4Model.sharding(1).create(hash_id: 1))
    assert_equal(1, Mod4Model.sharding(1).count)
    assert_equal(1, Mod4Model.sharding(0).count)
  end

  def test_touch
    m1 = Mod2Model.create(hash_id: 1)
    m2 = Mod2Model.sharding(1).first
    assert_equal(1, Mod2Model.sharding(1).count)
    assert_equal(0, Mod2Model.sharding(0).count)
    assert(m1.touch)
    assert(m2.touch)
    assert(m2.reload.touch)
    assert(m1.touch)
  end

  def test_touch_with_lock
    m1 = Mod4Model.create(hash_id: 1)
    m2 = Mod4Model.sharding(1).first
    assert_equal(1, Mod4Model.sharding(1).count)
    assert_equal(0, Mod4Model.sharding(0).count)
    assert(m1.touch)
    assert(m2.touch)
    assert(m2.reload.touch)
    assert(m1.touch)
  end

  def test_update
    m1 = Mod2Model.create(hash_id: 1)
    m2 = Mod2Model.sharding(1).first
    assert_equal(1, Mod2Model.sharding(1).count)
    assert_equal(0, Mod2Model.sharding(0).count)
    assert(m1.update(updated_at: Time.now))
    assert(m2.update(updated_at: Time.now))
    assert(m2.reload.update(updated_at: Time.now))
    assert(m1.reload.update(updated_at: Time.now))
  end

  def test_update_with_lock
    m1 = Mod4Model.create(hash_id: 1)
    m2 = Mod4Model.sharding(1).first
    assert_equal(1, Mod4Model.sharding(1).count)
    assert_equal(0, Mod4Model.sharding(0).count)
    assert(m1.update(updated_at: Time.now))
    assert_raises(ActiveRecord::StaleObjectError) { m2.update(updated_at: Time.now) }
    assert(m2.reload.update(updated_at: Time.now))
    assert_raises(ActiveRecord::StaleObjectError) { m1.update(updated_at: Time.now) }
    assert(m1.reload.update(updated_at: Time.now))
  end

  def test_update_wrong_sharding
    m = Mod2Model.create(hash_id: 0)
    assert_equal(0, Mod2Model.sharding(1).count)
    assert_equal(1, Mod2Model.sharding(0).count)
    assert_raises(ActsAsShardable::WrongShardingError) { m.update(hash_id: 1) }
    assert_equal(0, Mod2Model.sharding(1).count)
    assert_equal(1, Mod2Model.sharding(0).count)
  end

  def test_update_wrong_sharding_with_lock
    m = Mod4Model.create(hash_id: 0)
    assert_equal(0, Mod4Model.sharding(1).count)
    assert_equal(1, Mod4Model.sharding(0).count)
    assert_raises(ActsAsShardable::WrongShardingError) { m.update(hash_id: 1) }
    assert_equal(0, Mod4Model.sharding(1).count)
    assert_equal(1, Mod4Model.sharding(0).count)
  end

  def test_update_failed
    m = Mod4Model.create(hash_id: 0)
    m.freeze
    assert_equal(0, Mod4Model.sharding(1).count)
    assert_equal(1, Mod4Model.sharding(0).count)
    assert_raises(RuntimeError) { m.update(hash_id: 1) }
    assert_equal(0, Mod4Model.sharding(1).count)
    assert_equal(1, Mod4Model.sharding(0).count)
  end
end

class Mod2Model < ActiveRecord::Base
  acts_as_shardable by: :hash_id, mod: 2
end

class Mod4Model < ActiveRecord::Base
  acts_as_shardable by: :hash_id, mod: 4
end
