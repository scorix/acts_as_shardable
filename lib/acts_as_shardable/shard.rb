module ActsAsShardable
  module Shard
    extend ActiveSupport::Concern

    included do
      alias :_class :class

      def shard
        if defined? @attributes
          shard_value = self.attributes[_class.base_class.shard_method.to_s]
          if shard_value
            _class.base_class.sharding(shard_value)
          else
            _class
          end
        else
          _class
        end
      end

      alias :class :shard
    end

    class_methods do

      def sharding(column)
        i = case self.shard_method_using
            when Proc
              self.shard_method_using.call(column)
            when Symbol
              self.send(self.shard_method_using, column, self.shard_args)
            end

        constantize_class(i)
      end

      private

      def mod(current, mod:)
        current % mod
      end

      def constantize_class(i)
        class_name = "#{base_class.name.demodulize}_%04d" % i
        @@sharding_class ||= {}
        @@sharding_class[class_name] ||= mutex.synchronize do
          if module_name.const_defined?(class_name, false)
            module_name.const_get(class_name, false)
          else
            Class.new(base_class) do
              self.table_name = ("#{base_class.base_table_name}_%04d" % i)

              if base_class.respond_to?(:protobuf_message)
                self.protobuf_message base_class.protobuf_message

                # Create a .to_proto method on XXX::ActiveRecord_Relation
                self.const_get('ActiveRecord_Relation', false).class_exec do
                  def to_proto(*args)
                    msg_class = base_class.name.demodulize.pluralize
                    module_name = base_class.name.deconstantize.constantize
                    module_name::Messages.const_get(msg_class, false).new(msg_class.underscore => map { |r| r.to_proto(*args) })
                  end
                end
              end

            end.tap { |k| module_name.const_set(class_name, k) }
          end
        end
      end
    end
  end
end
