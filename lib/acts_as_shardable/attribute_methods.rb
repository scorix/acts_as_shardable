module ActsAsShardable
  module AttributeMethods
    private

    # @overload
    def _assign_attribute(k, v)
      if persisted? && k.to_s == self.class.base_class.shard_method.to_s
        raise WrongShardingError, "The sharding key #{k} can't be change"
      end

      super
    end
  end
end
