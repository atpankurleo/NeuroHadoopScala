package edu.pa;

@SuppressWarnings("all")
@org.apache.avro.specific.AvroGenerated
public class Rat extends org.apache.avro.specific.SpecificRecordBase implements org.apache.avro.specific.SpecificRecord {
  public static final org.apache.avro.Schema SCHEMA$ = new org.apache.avro.Schema.Parser().parse("{\"type\":\"record\",\"name\":\"Rat\",\"namespace\":\"edu.pa\",\"fields\":[{\"name\":\"time\",\"type\":\"int\"},{\"name\":\"frequency\",\"type\":\"int\"},{\"name\":\"convolution\",\"type\":\"float\"}]}");
  public static org.apache.avro.Schema getClassSchema() { return SCHEMA$; }
  @Deprecated public int time;
  @Deprecated public int frequency;
  @Deprecated public float convolution;

  /**
   * Default constructor.  Note that this does not initialize fields
   * to their default values from the schema.  If that is desired then
   * one should use {@link \#newBuilder()}. 
   */
  public Rat() {}

  /**
   * All-args constructor.
   */
  public Rat(Integer time, Integer frequency, Float convolution) {
    this.time = time;
    this.frequency = frequency;
    this.convolution = convolution;
  }

  public org.apache.avro.Schema getSchema() { return SCHEMA$; }
  // Used by DatumWriter.  Applications should not call.
  public Object get(int field$) {
    switch (field$) {
    case 0: return time;
    case 1: return frequency;
    case 2: return convolution;
    default: throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }
  // Used by DatumReader.  Applications should not call.
  @SuppressWarnings(value="unchecked")
  public void put(int field$, Object value$) {
    switch (field$) {
    case 0: time = (Integer)value$; break;
    case 1: frequency = (Integer)value$; break;
    case 2: convolution = (Float)value$; break;
    default: throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }

  /**
   * Gets the value of the 'time' field.
   */
  public Integer getTime() {
    return time;
  }

  /**
   * Sets the value of the 'time' field.
   * @param value the value to set.
   */
  public void setTime(Integer value) {
    this.time = value;
  }

  /**
   * Gets the value of the 'frequency' field.
   */
  public Integer getFrequency() {
    return frequency;
  }

  /**
   * Sets the value of the 'frequency' field.
   * @param value the value to set.
   */
  public void setFrequency(Integer value) {
    this.frequency = value;
  }

  /**
   * Gets the value of the 'convolution' field.
   */
  public Float getConvolution() {
    return convolution;
  }

  /**
   * Sets the value of the 'convolution' field.
   * @param value the value to set.
   */
  public void setConvolution(Float value) {
    this.convolution = value;
  }

  /** Creates a new Rat RecordBuilder */
  public static Rat.Builder newBuilder() {
    return new Rat.Builder();
  }

  /** Creates a new Rat RecordBuilder by copying an existing Builder */
  public static Rat.Builder newBuilder(Rat.Builder other) {
    return new Rat.Builder(other);
  }

  /** Creates a new Rat RecordBuilder by copying an existing Rat instance */
  public static Rat.Builder newBuilder(Rat other) {
    return new Rat.Builder(other);
  }

  /**
   * RecordBuilder for Rat instances.
   */
  public static class Builder extends org.apache.avro.specific.SpecificRecordBuilderBase<Rat>
    implements org.apache.avro.data.RecordBuilder<Rat> {

    private int time;
    private int frequency;
    private float convolution;

    /** Creates a new Builder */
    private Builder() {
      super(Rat.SCHEMA$);
    }

    /** Creates a Builder by copying an existing Builder */
    private Builder(Rat.Builder other) {
      super(other);
      if (isValidValue(fields()[0], other.time)) {
        this.time = data().deepCopy(fields()[0].schema(), other.time);
        fieldSetFlags()[0] = true;
      }
      if (isValidValue(fields()[1], other.frequency)) {
        this.frequency = data().deepCopy(fields()[1].schema(), other.frequency);
        fieldSetFlags()[1] = true;
      }
      if (isValidValue(fields()[2], other.convolution)) {
        this.convolution = data().deepCopy(fields()[2].schema(), other.convolution);
        fieldSetFlags()[2] = true;
      }
    }

    /** Creates a Builder by copying an existing Rat instance */
    private Builder(Rat other) {
            super(Rat.SCHEMA$);
      if (isValidValue(fields()[0], other.time)) {
        this.time = data().deepCopy(fields()[0].schema(), other.time);
        fieldSetFlags()[0] = true;
      }
      if (isValidValue(fields()[1], other.frequency)) {
        this.frequency = data().deepCopy(fields()[1].schema(), other.frequency);
        fieldSetFlags()[1] = true;
      }
      if (isValidValue(fields()[2], other.convolution)) {
        this.convolution = data().deepCopy(fields()[2].schema(), other.convolution);
        fieldSetFlags()[2] = true;
      }
    }

    /** Gets the value of the 'time' field */
    public Integer getTime() {
      return time;
    }

    /** Sets the value of the 'time' field */
    public Rat.Builder setTime(int value) {
      validate(fields()[0], value);
      this.time = value;
      fieldSetFlags()[0] = true;
      return this;
    }

    /** Checks whether the 'time' field has been set */
    public boolean hasTime() {
      return fieldSetFlags()[0];
    }

    /** Clears the value of the 'time' field */
    public Rat.Builder clearTime() {
      fieldSetFlags()[0] = false;
      return this;
    }

    /** Gets the value of the 'frequency' field */
    public Integer getFrequency() {
      return frequency;
    }

    /** Sets the value of the 'frequency' field */
    public Rat.Builder setFrequency(int value) {
      validate(fields()[1], value);
      this.frequency = value;
      fieldSetFlags()[1] = true;
      return this;
    }

    /** Checks whether the 'frequency' field has been set */
    public boolean hasFrequency() {
      return fieldSetFlags()[1];
    }

    /** Clears the value of the 'frequency' field */
    public Rat.Builder clearFrequency() {
      fieldSetFlags()[1] = false;
      return this;
    }

    /** Gets the value of the 'convolution' field */
    public Float getConvolution() {
      return convolution;
    }

    /** Sets the value of the 'convolution' field */
    public Rat.Builder setConvolution(float value) {
      validate(fields()[2], value);
      this.convolution = value;
      fieldSetFlags()[2] = true;
      return this;
    }

    /** Checks whether the 'convolution' field has been set */
    public boolean hasConvolution() {
      return fieldSetFlags()[2];
    }

    /** Clears the value of the 'convolution' field */
    public Rat.Builder clearConvolution() {
      fieldSetFlags()[2] = false;
      return this;
    }

    @Override
    public Rat build() {
      try {
        Rat record = new Rat();
        record.time = fieldSetFlags()[0] ? this.time : (Integer) defaultValue(fields()[0]);
        record.frequency = fieldSetFlags()[1] ? this.frequency : (Integer) defaultValue(fields()[1]);
        record.convolution = fieldSetFlags()[2] ? this.convolution : (Float) defaultValue(fields()[2]);
        return record;
      } catch (Exception e) {
        throw new org.apache.avro.AvroRuntimeException(e);
      }
    }
  }
}
