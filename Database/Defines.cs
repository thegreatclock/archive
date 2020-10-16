namespace GreatClock.Common.RTS.DB {

	public enum eDataType {
		Empty = 0, JumpTo = 1, Null = 2, Byte = 3, SByte = 4, UShort = 5, Short = 6, UInt = 7,
		Int = 8, ULong = 9, Long = 10, Float = 11, Double = 12, Bool = 13, String = 14, Dict = 15
	}

	public interface IDataReader {
		bool Reset();
		bool Read(out string key, out eDataType type, out object value);
	}

}
