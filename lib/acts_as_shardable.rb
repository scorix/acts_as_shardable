require 'active_record'
require "acts_as_shardable/version"
require "acts_as_shardable/attribute_methods"
require "acts_as_shardable/shard"

module ActsAsShardable

  class WrongShardingError < ::ActiveRecord::ActiveRecordError
  end

  mattr_reader :mutex do
    Mutex.new
  end

  def acts_as_shardable(by:, using:, args: {})
    class_attribute :shard_method, :shard_method_using, :shard_args, :module_name, :base_table_name

    case using
    when Symbol
      # built-in functions
    when Proc
      # customized functions
    else
      raise ArgumentError, "unknown sharding function, `using` must be a symbol or proc"
    end

    mutex.synchronize do
      self.shard_method = by
      self.shard_method_using = using
      self.shard_args = args
      self.module_name = self.name.deconstantize.safe_constantize || Object
      self.base_table_name = self.name.demodulize.pluralize.underscore
      self.table_name = "#{self.base_table_name}_0000"
      self.validates self.shard_method.to_sym, presence: true
    end

    include AttributeMethods
    include Shard
  end

end


# Extend ActiveRecord's functionality
ActiveRecord::Base.send :extend, ActsAsShardable
