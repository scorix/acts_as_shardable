require 'active_record'
require "acts_as_shardable/version"

module ActsAsShardable

  class WrongShardingError < ::ActiveRecord::ActiveRecordError
  end

  mattr_reader :mutex do
    Mutex.new
  end

  def acts_as_shardable(column:, mod:)
    class_attribute :shard_column, :shard_mod, :module_name, :base_table_name

    mutex.synchronize do
      self.shard_column = column
      self.shard_mod = mod
      self.module_name = self.name.deconstantize.safe_constantize || Object
      self.base_table_name = self.name.demodulize.pluralize.underscore
      self.table_name = "#{self.base_table_name}_0000"
      self.validates self.shard_column.to_sym, presence: true

      # Updates the associated record with values matching those of the instance attributes.
      # Returns the number of affected rows.
      define_method :_update_record do |attribute_names = self.attribute_names|
        attributes_values = arel_attributes_with_values_for_update(attribute_names)
        if attributes_values.empty?
          0
        else
          was, is = changes[self.class.base_class.shard_column]
          if was
            transaction do
              self.class.base_class.sharding(is).unscoped.insert attributes_values

              raise ReadOnlyRecord, "#{self.class.base_class.sharding(was)} is marked as readonly" if readonly?
              destroy_associations
              destroy_row if persisted?
              @destroyed = true
              freeze
            end

            1
          else
            self.class.base_class.sharding(self[self.class.base_class.shard_column]).unscoped._update_record attributes_values, id, id_was
          end
        end

        # was, is = changes[self.class.base_class.shard_column]
        # if !new_record? && was
        #   destroy
        #   o = self.class.base_class.sharding(is)
        #   becomes(o).save(*args)
        # elsif self.class < self.class.base_class
        #   super(*args)
        # else
        #   o = self.class.base_class.sharding(self[self.class.base_class.shard_column])
        #   becomes(o).save(*args)
        # end
      end

      private :_update_record

      # Creates a record with values matching those of the instance attributes
      # and returns its id.
      define_method :_create_record do |attribute_names = self.attribute_names|
        attributes_values = arel_attributes_with_values_for_create(attribute_names)

        new_id = self.class.base_class.sharding(self[self.class.base_class.shard_column]).unscoped.insert attributes_values
        self.id ||= new_id if self.class.base_class.primary_key

        @new_record = false
        id
      end

      private :_create_record

      self.class.send :define_method, :sharding do |column|
        i = column.to_i % shard_mod
        klass = "#{base_class.name.demodulize}_%04d" % i
        @@sharding_class ||= {}
        @@sharding_class[klass] ||= mutex.synchronize do
          if module_name.const_defined?(klass)
            module_name.const_get(klass)
          else
            Class.new(base_class) do
              self.table_name = ("#{base_class.base_table_name}_%04d" % i)

              if base_class.respond_to?(:protobuf_message)
                self.protobuf_message base_class.protobuf_message

                # Create a .to_proto method on XXX::ActiveRecord_Relation
                self.const_get('ActiveRecord_Relation').class_exec do
                  def to_proto(*args)
                    msg_class = base_class.name.demodulize.pluralize
                    Messages.const_get(msg_class).new(msg_class.underscore => map { |r| r.to_proto(*args) })
                  end
                end
              end

            end.tap { |k| module_name.const_set(klass, k) }
          end
        end
      end
    end
  end

end


# Extend ActiveRecord's functionality
ActiveRecord::Base.send :extend, ActsAsShardable
