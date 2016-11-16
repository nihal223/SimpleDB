package simpledb;

import java.text.ParseException;
import java.io.*;

/**
 * Class representing a type in SimpleDB.
 * Types are static objects defined by this class; hence, the Type
 * constructor is private.
 */
public class Type {
  int typeId;
  int len;

  private Type(int typeId) {
    this.typeId = typeId;

  }

  /**
   * @return the number of bytes required to store a field of this type.
   */
  public int getLen() {
    switch (typeId) {
      case INT_ID:
        return 4;
    case STRING_ID:
    return STRING_LEN + 4;
      default:
        return 0;
    }
  }

  /**
   * @return a Field object of the same type as this object that has contents
   *   read from the specified DataInputStream.
   * @param dis The input stream to read from
   * @throws ParseException if the data read from the input stream is not
   *   of the appropriate type.
   */
  public Field parse(DataInputStream dis) throws ParseException {
    try {
      switch (typeId) {
        case INT_ID:
          return new IntField(dis.readInt());
      case STRING_ID:
      int strLen = dis.readInt();
      byte bs[] = new byte[strLen];
      dis.read(bs);
      dis.skipBytes(STRING_LEN-strLen);
      return new StringField(new String(bs), STRING_LEN);
        default:
          return null;
      }
    } catch (IOException e) {
	throw new ParseException("couldn't parse"+ typeId+":", 0);
    }
  }

  /**
   * @return true if the specified type is the same as the type of this object
   */
  public boolean equals(Type t) {
    return t.typeId == typeId;
  }

  static final int INT_ID = 1;
  static final int STRING_ID = 2;
 public   static final int STRING_LEN = 128;

  /**
   * Type object representing integers and strings (the only types currently supported by
   * SimpleDB)
   */
    public static final Type INT_TYPE = new Type(INT_ID);
    public static final Type STRING_TYPE = new Type(STRING_ID);

    public String toString() {
    switch(typeId) {
    case INT_ID:
        return "INT";
    case STRING_ID:
        return "STRING";
    }
    return "";
    }
}
