using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Text;

namespace GreatClock.Common.RTS.DB {

	public sealed class KVDB : IDisposable {

		private readonly int mSegmentShift;

		private object mLocker = new object();

		private EncriptedStream mStream;
		private Buffer mBuffer = new Buffer();
		private int mEmptySeg;
		private byte[] mBlockBuffer;

		private CachedDatas mRoot;

		public KVDB(int segmentShift, string path) {
			if (string.IsNullOrEmpty(path)) {
				throw new ArgumentNullException("path");
			}
			if (!File.Exists(path)) {
				string dir = Path.GetDirectoryName(path);
				if (!Directory.Exists(dir)) {
					throw new Exception("Invalid path");
				}
			}
			mStream = new EncriptedStream(File.Open(path, FileMode.OpenOrCreate));
			if (mStream.Length <= 0L) {
				if (segmentShift < 5 || segmentShift > 10) {
					mStream.Dispose();
					throw new ArgumentException("segmentShift");
				}
				mSegmentShift = segmentShift;
				int blockSize = 1 << mSegmentShift;
				mBlockBuffer = new byte[blockSize];
				mBlockBuffer[0] = (byte)((int)eDataType.Empty << 4);
				mBlockBuffer[1] = (byte)7;
				mBlockBuffer[6] = (byte)segmentShift;
				mStream.Write(mBlockBuffer, 0, blockSize);
				mStream.Flush();
				mEmptySeg = 0;
			} else {
				byte[] tb = new byte[5];
				mStream.Position = 2L;
				mStream.Read(tb, 0, 5);
				mEmptySeg = (int)(((uint)tb[0] << 24) | ((uint)tb[1] << 16) | ((uint)tb[2] << 8) | (uint)tb[3]);
				mSegmentShift = tb[4];
				mBlockBuffer = new byte[1 << mSegmentShift];
			}
			mBlockBuffer.Initialize();
			mRoot = new CachedDatas(this, 0, 0, true);
			mRoot.Cache(false, 1);
		}

		public bool Contains(string key) {
			lock (mLocker) {
				bool ret = mRoot.Contains(key, 0);
				CachedDatas.RemoveTimeouts();
				return ret;
			}
		}

		public bool GetData(string key, out eDataType type, out object value) {
			lock (mLocker) {
				bool ret = mRoot.GetData(key, 0, out type, out value);
				CachedDatas.RemoveTimeouts();
				return ret;
			}
		}

		public bool SetData(string key, eDataType type, object value) {
			lock (mLocker) {
				if (mRoot.SetData(key, 0, type, value)) {
					ProcessAll();
					mStream.Flush();
					CachedDatas.RemoveTimeouts();
					return true;
				}
				ProcessUndoAll();
			}
			return false;
		}

		public bool SetData(DataItem item) {
			lock (mLocker) {
				if (mRoot.SetData(item.key, 0, item.type, item.value)) {
					ProcessAll();
					mStream.Flush();
					CachedDatas.RemoveTimeouts();
					return true;
				}
				ProcessUndoAll();
			}
			return false;
		}

		public bool SetData(IEnumerable<DataItem> items, out int failedAt) {
			if (items == null) { failedAt = -1; return false; }
			lock (mLocker) {
				int n = 0;
				foreach (DataItem item in items) {
					if (!mRoot.SetData(item.key, 0, item.type, item.value)) {
						ProcessUndoAll();
						failedAt = n;
						return false;
					}
					n++;
				}
				ProcessAll();
				mStream.Flush();
				CachedDatas.RemoveTimeouts();
			}
			failedAt = -1;
			return true;
		}

		public bool DeleteData(string key) {
			lock (mLocker) {
				if (mRoot.SetData(key, 0, eDataType.Empty, null)) {
					ProcessAll();
					mStream.Flush();
					CachedDatas.RemoveTimeouts();
					return true;
				}
				ProcessUndoAll();
			}
			return false;
		}

		public void Dispose() {
			mStream.Dispose();
		}

		public struct DataItem {
			public string key;
			public eDataType type;
			public object value;
			public DataItem(string key, eDataType type, object value) {
				this.key = key;
				this.type = type;
				this.value = value;
			}
		}

		private int GetEmptySegment() {
			long cachedPos = mStream.Position;
			int ret = mEmptySeg;
			if (mEmptySeg <= 0) {
				int blockSize = 1 << mSegmentShift;
				ret = (int)((mStream.Length + blockSize - 1) / blockSize);
				long pos = (long)ret << mSegmentShift;
				mStream.Position = mStream.Length;
				mStream.Write(mBlockBuffer, 0, (int)(pos - mStream.Position));
				mStream.Write(mBlockBuffer, 0, blockSize);
			} else {
				mStream.Position = ret << mSegmentShift;
				mStream.Read(mBlockBuffer, 0, 4);
				mEmptySeg = (int)(((uint)mBlockBuffer[0] << 24) | ((uint)mBlockBuffer[1] << 16) | ((uint)mBlockBuffer[2] << 8) | (uint)mBlockBuffer[3]);
				mBlockBuffer.Initialize();
				mStream.Position = ret << mSegmentShift;
				mStream.Write(mBlockBuffer, 0, 4);
				mBlockBuffer[0] = (byte)(mEmptySeg >> 24);
				mBlockBuffer[1] = (byte)(mEmptySeg >> 16);
				mBlockBuffer[2] = (byte)(mEmptySeg >> 8);
				mBlockBuffer[3] = (byte)mEmptySeg;
				mStream.Position = 2L;
				mStream.Write(mBlockBuffer, 0, 4);
			}
			mStream.Position = cachedPos;
			return ret;
		}

		private void SetSegmentUnused(int seg) {
			long cachedPos = mStream.Position;
			mBlockBuffer[0] = (byte)(mEmptySeg >> 24);
			mBlockBuffer[1] = (byte)(mEmptySeg >> 16);
			mBlockBuffer[2] = (byte)(mEmptySeg >> 8);
			mBlockBuffer[3] = (byte)mEmptySeg;
			mStream.Position = (long)seg << mSegmentShift;
			mStream.Write(mBlockBuffer, 0, 4);
			mEmptySeg = seg;
			mBlockBuffer[0] = (byte)(mEmptySeg >> 24);
			mBlockBuffer[1] = (byte)(mEmptySeg >> 16);
			mBlockBuffer[2] = (byte)(mEmptySeg >> 8);
			mBlockBuffer[3] = (byte)mEmptySeg;
			mStream.Position = 2L;
			mStream.Write(mBlockBuffer, 0, 4);
			mStream.Position = cachedPos;
		}

		private static int GetTypeSize(eDataType type) {
			switch (type) {
				case eDataType.JumpTo:
					return 4;
				case eDataType.Null:
					return 5;
				case eDataType.Byte:
				case eDataType.SByte:
					return 6;
				case eDataType.UShort:
				case eDataType.Short:
					return 7;
				case eDataType.UInt:
				case eDataType.Int:
					return 9;
				case eDataType.ULong:
				case eDataType.Long:
					return 13;
				case eDataType.Float:
					return 15;
				case eDataType.Double:
					return 19;
				case eDataType.Bool:
					return 6;
				case eDataType.String:
					return 11;
				case eDataType.Dict:
					return 9;
			}
			return -1;
		}

		private static int GetBCD(byte chr) {
			switch ((int)chr) {
				case 43: return 12; // +
				case 45: return 13; // -
				case 46: return 10; // .
				case 48:
				case 49:
				case 50:
				case 51:
				case 52:
				case 53:
				case 54:
				case 55:
				case 56:
				case 57:
					return chr - 48;
				case 69:
				case 101:
					return 11;
			}
			return -1;
		}

		private static byte GetASCIIFromBCD(int n) {
			switch (n) {
				case 0:
				case 1:
				case 2:
				case 3:
				case 4:
				case 5:
				case 6:
				case 7:
				case 8:
				case 9:
					return (byte)(48 + n);
				case 10:
					return (byte)46;
				case 11:
					return (byte)69;
				case 12:
					return (byte)43;
				case 13:
					return (byte)45;
			}
			return (byte)255;
		}

		private static List<TimeSpan> cached_life_times = null;
		private static TimeSpan GetCacheLifeTime(int depth) {
			if (cached_life_times == null) {
				cached_life_times = new List<TimeSpan>(8);
				cached_life_times.Add(TimeSpan.FromDays(365000.0));
			}
			while (depth >= cached_life_times.Count) {
				cached_life_times.Add(TimeSpan.FromSeconds(90f / depth + 10f));
			}
			return cached_life_times[depth];
		}

		private class Buffer {
			private byte[] mBuffer512 = new byte[512];
			public byte[] buffer512 { get { return mBuffer512; } }
			private byte[] mBuffer;
			public byte[] Get(int min) {
				int s = mBuffer == null ? 0 : mBuffer.Length;
				int ns = s <= 0 ? 256 : s;
				while (min > ns) { ns <<= 1; }
				if (ns != s) { mBuffer = new byte[ns]; }
				return mBuffer;
			}
		}

		private enum eCacheState { None, DictOnly, All }

		private interface IHelper {
			int SegmentShift { get; }
			EncriptedStream stream { get; }
			int GetEmptySegment();
			byte[] buffer512 { get; }
			CachedDatas AddChildNode(Node node, IDataReader children);
			void Free(long pos, bool recursive);
			long GetExtendFromPos(Node node, int size);
			int WriteString(string str, int seg);
		}

		private interface IProcess {
			void Flush();
			void Undo();
		}

		private Queue<IProcess> mProcessQueue = new Queue<IProcess>(128);

		private void ProcessEnqueue(IProcess process) {
			if (process != null) { mProcessQueue.Enqueue(process); }
		}

		private void ProcessAll() {
			while (mProcessQueue.Count > 0) {
				IProcess process = mProcessQueue.Dequeue();
				process.Flush();
			}
		}

		private void ProcessUndoAll() {
			while (mProcessQueue.Count > 0) {
				IProcess process = mProcessQueue.Dequeue();
				process.Undo();
			}
		}

		private class Node : IProcess {

			public readonly string key;
			private readonly IHelper helper;

			private eDataType mType_;
			public eDataType type { get; private set; }

			private object mData_;
			public object data { get; private set; }

			public long pos { get; private set; }

			private Node(string key, IHelper helper) { this.key = key; this.helper = helper; }

			public static Node CreateNodeFromDB(IHelper helper, string key, long pos, eDataType type, object value) {
				Node n = new Node(key, helper);
				n.pos = pos;
				n.type = type;
				n.data = value;
				n.mType_ = type;
				n.mData_ = value;
				return n;
			}

			public static Node CreateNewNode(IHelper helper, string key) {
				Node n = new Node(key, helper);
				n.pos = 0L;
				n.mType_ = eDataType.Empty;
				n.mData_ = null;
				return n;
			}

			public bool SetData(eDataType type, object value) {
				object d = value;
				switch (type) {
					case eDataType.Empty:
					case eDataType.Null: break;
					case eDataType.Byte: if (!(value is byte)) { return false; } break;
					case eDataType.SByte: if (!(value is sbyte)) { return false; } break;
					case eDataType.UShort: if (!(value is ushort)) { return false; } break;
					case eDataType.Short: if (!(value is short)) { return false; } break;
					case eDataType.UInt: if (!(value is uint)) { return false; } break;
					case eDataType.Int: if (!(value is int)) { return false; } break;
					case eDataType.ULong: if (!(value is ulong)) { return false; } break;
					case eDataType.Long: if (!(value is long)) { return false; } break;
					case eDataType.Float: if (!(value is float)) { return false; } break;
					case eDataType.Double: if (!(value is double)) { return false; } break;
					case eDataType.Bool: if (!(value is bool)) { return false; } break;
					case eDataType.String:
						if (value == null) { type = eDataType.Null; break; }
						if (!(value is string)) { return false; }
						break;
					case eDataType.Dict:
						if (value == null) { type = eDataType.Null; break; }
						IDataReader reader = value as IDataReader;
						if (reader == null) { return false; }
						d = helper.AddChildNode(this, reader);
						break;
					default:
						return false;
				}
				if (type != this.type) {
					if (this.type == eDataType.Dict) {
						CachedDatas cd = data as CachedDatas;
						if (cd != null) { cd.Release(); }
					}
					this.type = type;
				}
				data = d;
				return true;
			}

			public void Release() {
				type = eDataType.Empty;
				mType_ = eDataType.Empty;
			}

			public bool ToProcess {
				get {
					if (type == eDataType.Empty && mType_ == eDataType.Empty) { return false; }
					if (type == mType_ && (data == mData_ || Equals(data, mData_))) { return false; }
					return true;
				}
			}

			public void Free() {
				if (pos > 0L) {
					helper.Free(pos, false);
					pos = 0L;
				}
			}

			public void Write(bool writeKey) {
				if (pos <= 0L) {
					pos = helper.GetExtendFromPos(this, GetTypeSize(type));
					writeKey = true;
				}
				byte[] buffer = helper.buffer512;
				int keySeg = -1;
				int keyLen = -1;
				if (writeKey) {
					keyLen = Encoding.ASCII.GetBytes(key, 0, key.Length, buffer, 32);
					if (keyLen <= 4) {
						uint val = (uint)type << 28;
						int shift = 21;
						for (int i = 0; i < keyLen; i++) {
							val |= (uint)buffer[32 + i] << shift;
							shift -= 7;
						}
						buffer[0] = (byte)(((uint)type << 4) | (val >> 24));
						buffer[1] = (byte)(val >> 16);
						buffer[2] = (byte)(val >> 8);
						buffer[3] = (byte)val;
						buffer[4] = (byte)keyLen;
					} else {
						keySeg = helper.GetEmptySegment();
						buffer[0] = (byte)(((int)type << 4) | (keySeg >> 24));
						buffer[1] = (byte)(keySeg >> 16);
						buffer[2] = (byte)(keySeg >> 8);
						buffer[3] = (byte)keySeg;
						buffer[4] = (byte)keyLen;
					}
				}
				switch (type) {
					case eDataType.Null: break;
					case eDataType.Byte:
						buffer[5] = (byte)data;
						break;
					case eDataType.SByte:
						buffer[5] = (byte)(sbyte)data;
						break;
					case eDataType.UShort:
						ushort valUS = (ushort)data;
						buffer[5] = (byte)(valUS >> 8);
						buffer[6] = (byte)valUS;
						break;
					case eDataType.Short:
						short valS = (short)data;
						buffer[5] = (byte)(valS >> 8);
						buffer[6] = (byte)valS;
						break;
					case eDataType.UInt:
						uint valUI = (uint)data;
						buffer[5] = (byte)(valUI >> 24);
						buffer[6] = (byte)(valUI >> 16);
						buffer[7] = (byte)(valUI >> 8);
						buffer[8] = (byte)valUI;
						break;
					case eDataType.Int:
						int valI = (int)data;
						buffer[5] = (byte)(valI >> 24);
						buffer[6] = (byte)(valI >> 16);
						buffer[7] = (byte)(valI >> 8);
						buffer[8] = (byte)valI;
						break;
					case eDataType.ULong:
						ulong valUL = (ulong)data;
						buffer[5] = (byte)(valUL >> 56);
						buffer[6] = (byte)(valUL >> 48);
						buffer[7] = (byte)(valUL >> 40);
						buffer[8] = (byte)(valUL >> 32);
						buffer[9] = (byte)(valUL >> 24);
						buffer[10] = (byte)(valUL >> 16);
						buffer[11] = (byte)(valUL >> 8); ;
						buffer[12] = (byte)valUL;
						break;
					case eDataType.Long:
						long valL = (long)data;
						buffer[5] = (byte)(valL >> 56);
						buffer[6] = (byte)(valL >> 48);
						buffer[7] = (byte)(valL >> 40);
						buffer[8] = (byte)(valL >> 32);
						buffer[9] = (byte)(valL >> 24);
						buffer[10] = (byte)(valL >> 16);
						buffer[11] = (byte)(valL >> 8);
						buffer[12] = (byte)valL;
						break;
					case eDataType.Float:
						string fs = ((float)data).ToString("R", CultureInfo.InvariantCulture);
						int lenFS = Encoding.ASCII.GetBytes(fs, 0, fs.Length, buffer, 448);
						buffer[5] = (byte)(lenFS >> 8);
						buffer[6] = (byte)lenFS;
						for (int i = 0, imax = (lenFS + 1) >> 1; i < imax; i++) {
							int ii = 448 + (i << 1);
							buffer[7 + i] = (byte)((GetBCD(buffer[ii]) << 4) | GetBCD(buffer[ii + 1]));
						}
						break;
					case eDataType.Double:
						string ds = ((double)data).ToString("R", CultureInfo.InvariantCulture);
						int lenDS = Encoding.ASCII.GetBytes(ds, 0, ds.Length, buffer, 448);
						buffer[5] = (byte)(lenDS >> 8);
						buffer[6] = (byte)lenDS;
						for (int i = 0, imax = (lenDS + 1) >> 1; i < imax; i++) {
							int ii = 448 + (i << 1);
							buffer[7 + i] = (byte)((GetBCD(buffer[ii]) << 4) | GetBCD(buffer[ii + 1]));
						}
						break;
					case eDataType.Bool:
						buffer[5] = (byte)((bool)data ? 1 : 0);
						break;
					case eDataType.String:
						string str = (string)data;
						int strLen = str.Length;
						if (strLen <= 3) {
							buffer[500] = buffer[501] = buffer[502] = 0;
							strLen = Encoding.UTF8.GetBytes(str, 0, strLen, buffer, 500);
						}
						if (strLen <= 3) {
							buffer[5] = (byte)(strLen >> 12);
							buffer[6] = (byte)(strLen >> 4);
							buffer[7] = (byte)(strLen << 4);
							buffer[8] = buffer[500];
							buffer[9] = buffer[501];
							buffer[10] = buffer[502];
						} else {
							int stringSeg = helper.GetEmptySegment();
							strLen = helper.WriteString(str, stringSeg);
							buffer[5] = (byte)(strLen >> 12);
							buffer[6] = (byte)(strLen >> 4);
							buffer[7] = (byte)((strLen << 4) | stringSeg >> 24);
							buffer[8] = (byte)(stringSeg >> 16);
							buffer[9] = (byte)(stringSeg >> 8);
							buffer[10] = (byte)stringSeg;
						}
						break;
					case eDataType.Dict:
						int dictSeg = (data as CachedDatas).seg;
						buffer[5] = (byte)(dictSeg >> 24);
						buffer[6] = (byte)(dictSeg >> 16);
						buffer[7] = (byte)(dictSeg >> 8);
						buffer[8] = (byte)dictSeg;
						break;
				}
				EncriptedStream stream = helper.stream;
				if (keySeg > 0) {
					stream.Position = keySeg << helper.SegmentShift;
					stream.Write(buffer, 32, keyLen);
				}
				stream.Position = writeKey ? pos : (pos + 5);
				stream.Write(buffer, writeKey ? 0 : 5, writeKey ? GetTypeSize(type) : (GetTypeSize(type) - 5));
			}

			void IProcess.Flush() {
				if (!ToProcess) { return; }
				if (type != mType_ && pos > 0L) {
					helper.Free(pos, true);
					pos = 0L;
				}
				if (type != eDataType.Empty) { Write(type != mType_); }
				mType_ = type;
				mData_ = data;
			}

			void IProcess.Undo() {
				if (mType_ == eDataType.Empty) { return; }
				type = mType_;
				data = mData_;
			}

		}

		private class CachedDatas : IDataReader, IHelper, IProcess {

			public readonly int depth;
			private List<int> mSegs = new List<int>();
			private KVDB mInstance;
			private long mEndPos;
			private Dictionary<string, Node> mDatas;
			private List<string> mKeys;
			private Dictionary<string, Node> mDatas_ = null;
			private List<string> mKeys_ = null;
			private int mEmpties;
			private int mItor = -1;

			public CachedDatas(KVDB instance, int depth, int seg, bool fromStream) {
				mInstance = instance;
				this.depth = depth;
				if (seg >= 0) {
					mSegs.Add(seg);
					mEndPos = (long)seg << mInstance.mSegmentShift;
				}
				Visit();
				CachedState = fromStream ? eCacheState.None : eCacheState.All;
				mDatas = new Dictionary<string, Node>(16);
				mKeys = new List<string>(16);
				mEmpties = 0;
			}

			public int seg {
				get { CheckSegmentAndPosition(); return mSegs[0]; }
			}

			private eCacheState mCachedState = eCacheState.None;
			public eCacheState CachedState {
				get {
					return mCachedState;
				}
				private set {
					if (mCachedState == value) { return; }
					if (value == eCacheState.None) { RemoveCachedDatas(this); } else if (depth > 0 && mCachedState == eCacheState.None) { AddCachedDatas(this); }
					mCachedState = value;
				}
			}

			private void Visit() {
				Timeout = DateTime.UtcNow + GetCacheLifeTime(depth);
				SortCachedData(this);
			}
			private DateTime Timeout;

			int IHelper.SegmentShift { get { return mInstance.mSegmentShift; } }

			EncriptedStream IHelper.stream { get { return mInstance.mStream; } }

			byte[] IHelper.buffer512 { get { return mInstance.mBuffer.buffer512; } }

			public void Release() {
				foreach (KeyValuePair<string, Node> kv in mDatas) {
					kv.Value.Release();
				}
			}

			public void Cache(bool dictOnly, int depthToCache) {
				if (dictOnly ? CachedState != eCacheState.None : CachedState == eCacheState.All) {
					if (depthToCache != 0) {
						foreach (KeyValuePair<string, Node> kv in mDatas) {
							CachedDatas d = kv.Value.data as CachedDatas;
							if (d == null) { continue; }
							d.Cache(dictOnly, depthToCache > 0 ? depthToCache - 1 : -1);
						}
					}
					return;
				}
				bool firstCache = CachedState == eCacheState.None;
				bool readValue = !dictOnly;
				CachedState = dictOnly ? eCacheState.DictOnly : eCacheState.All;
				byte[] buffer = mInstance.mBuffer.buffer512;
				long pos = (long)seg << mInstance.mSegmentShift;
				if (pos >= mInstance.mStream.Length) { throw new Exception("Invalid database"); }
				mInstance.mStream.Position = pos;
				while (true) {
					if (mInstance.mStream.Read(buffer, 0, 1) != 1) { throw new Exception("Invalid database"); }
					eDataType type = (eDataType)((uint)buffer[0] >> 4);
					if (type == eDataType.Empty) {
						if (mInstance.mStream.Read(buffer, 1, 1) != 1) { throw new Exception("Invalid database"); }
						int es = buffer[1] & 0x0ff;
						if (es <= 0) {
							mEndPos = mInstance.mStream.Position - 2L;
							break;
						}
						if (mInstance.mStream.Read(buffer, 2, es - 2) != es - 2) { throw new Exception("Invalid database"); }
						if (firstCache) { mEmpties++; }
						continue;
					}
					if (mInstance.mStream.Read(buffer, 1, 4) != 4) { throw new Exception("Invalid database"); }
					if (type == eDataType.JumpTo) {
						int nseg = (int)((((uint)buffer[0] & 0x0f) << 24) | ((uint)buffer[1] << 16) | ((uint)buffer[2] << 8) | (uint)buffer[3]);
						long npos = nseg << mInstance.mSegmentShift;
						mInstance.mStream.Position = npos;
						if (firstCache) { mSegs.Add(nseg); }
						continue;
					}
					string key = null;
					if (type == eDataType.Dict ? firstCache : readValue) {
						int keyLen = buffer[4];
						uint kb = (((uint)buffer[0] & 0x0fu) << 24) | ((uint)buffer[1] << 16) | ((uint)buffer[2] << 8) | (uint)buffer[3];
						if (keyLen <= 4) {
							buffer[448] = (byte)((kb >> 21) & 0x7fu);
							buffer[449] = (byte)((kb >> 14) & 0x7fu);
							buffer[450] = (byte)((kb >> 7) & 0x7fu);
							buffer[451] = (byte)(kb & 0x7fu);
							key = Encoding.ASCII.GetString(buffer, 448, keyLen);
						} else {
							long cachedPos = mInstance.mStream.Position;
							key = ReadString(keyLen, (int)kb);
							mInstance.mStream.Position = cachedPos;
						}
					}
					object value = null;
					long valuePos = mInstance.mStream.Position - 5L;
					switch (type) {
						case eDataType.Byte:
							if (mInstance.mStream.Read(buffer, 5, 1) != 1) { throw new Exception("Invalid database"); }
							if (readValue) {
								value = buffer[5];
							}
							break;
						case eDataType.SByte:
							if (mInstance.mStream.Read(buffer, 5, 1) != 1) { throw new Exception("Invalid database"); }
							if (readValue) {
								value = (sbyte)buffer[5];
							}
							break;
						case eDataType.UShort:
							if (mInstance.mStream.Read(buffer, 5, 2) != 2) { throw new Exception("Invalid database"); }
							if (readValue) {
								value = (ushort)(((uint)buffer[5] << 8) | (uint)buffer[6]);
							}
							break;
						case eDataType.Short:
							if (mInstance.mStream.Read(buffer, 5, 2) != 2) { throw new Exception("Invalid database"); }
							if (readValue) {
								value = (short)(((uint)buffer[5] << 8) | (uint)buffer[6]);
							}
							break;
						case eDataType.UInt:
							if (mInstance.mStream.Read(buffer, 5, 4) != 4) { throw new Exception("Invalid database"); }
							if (readValue) {
								value = ((uint)buffer[5] << 24) | ((uint)buffer[6] << 16) | ((uint)buffer[7] << 8) | (uint)buffer[8];
							}
							break;
						case eDataType.Int:
							if (mInstance.mStream.Read(buffer, 5, 4) != 4) { throw new Exception("Invalid database"); }
							if (readValue) {
								value = (int)(((uint)buffer[5] << 24) | ((uint)buffer[6] << 16) | ((uint)buffer[7] << 8) | (uint)buffer[8]);
							}
							break;
						case eDataType.ULong:
							if (mInstance.mStream.Read(buffer, 5, 8) != 8) { throw new Exception("Invalid database"); }
							if (readValue) {
								value = ((ulong)buffer[5] << 56) | ((ulong)buffer[6] << 48) | ((ulong)buffer[7] << 40) | ((ulong)buffer[8] << 32) | ((ulong)buffer[9] << 24) | ((ulong)buffer[10] << 16) | ((ulong)buffer[11] << 8) | (ulong)buffer[12];
							}
							break;
						case eDataType.Long:
							if (mInstance.mStream.Read(buffer, 5, 8) != 8) { throw new Exception("Invalid database"); }
							if (readValue) {
								value = (long)(((ulong)buffer[5] << 56) | ((ulong)buffer[6] << 48) | ((ulong)buffer[7] << 40) | ((ulong)buffer[8] << 32) | ((ulong)buffer[9] << 24) | ((ulong)buffer[10] << 16) | ((ulong)buffer[11] << 8) | (ulong)buffer[12]);
							}
							break;
						case eDataType.Float:
							if (mInstance.mStream.Read(buffer, 5, 10) != 10) { throw new Exception("Invalid database"); }
							if (readValue) {
								int lf = (int)(((uint)buffer[5] << 8) | (uint)buffer[6]);
								for (int i = 0; i < lf; i++) {
									byte b = buffer[7 + (i >> 1)];
									buffer[448 + i] = GetASCIIFromBCD(((i & 1) == 0 ? b >> 4 : b) & 0x0f);
								}
								string sf = Encoding.ASCII.GetString(buffer, 448, lf);
								float fv;
								float.TryParse(sf, NumberStyles.Float, CultureInfo.InvariantCulture, out fv);
								value = fv;
							}
							break;
						case eDataType.Double:
							if (mInstance.mStream.Read(buffer, 5, 14) != 14) { throw new Exception("Invalid database"); }
							if (readValue) {
								int ld = (int)(((uint)buffer[5] << 8) | (uint)buffer[6]);
								for (int i = 0; i < ld; i++) {
									byte b = buffer[7 + (i >> 1)];
									buffer[448 + i] = GetASCIIFromBCD(((i & 1) == 0 ? b >> 4 : b) & 0x0f);
								}
								string sf = Encoding.ASCII.GetString(buffer, 448, ld);
								double dv;
								double.TryParse(sf, NumberStyles.Float, CultureInfo.InvariantCulture, out dv);
								value = dv;
							}
							break;
						case eDataType.Bool:
							if (mInstance.mStream.Read(buffer, 5, 1) != 1) { throw new Exception("Invalid database"); }
							if (readValue) {
								value = buffer[5] != 0;
							}
							break;
						case eDataType.String:
							if (mInstance.mStream.Read(buffer, 5, 6) != 6) { throw new Exception("Invalid database"); }
							if (readValue) {
								int sl = (int)(((uint)buffer[5] << 12) | ((uint)buffer[6] << 4) | ((uint)buffer[7] >> 4));
								if (sl <= 3) {
									value = Encoding.UTF8.GetString(buffer, 8, sl);
								} else {
									uint ss = (((uint)buffer[7] & 0x0fu) << 24) | ((uint)buffer[8] << 16) | ((uint)buffer[9] << 8) | (uint)buffer[10];
									long cachedPos = mInstance.mStream.Position;
									value = ReadString(sl, (int)ss);
									mInstance.mStream.Position = cachedPos;
								}
							}
							break;
						case eDataType.Dict:
							if (mInstance.mStream.Read(buffer, 5, 4) != 4) { throw new Exception("Invalid database"); }
							if (firstCache) {
								uint v = ((uint)buffer[5] << 24) | ((uint)buffer[6] << 16) | ((uint)buffer[7] << 8) | (uint)buffer[8];
								long cachedPos = mInstance.mStream.Position;
								CachedDatas d = new CachedDatas(mInstance, depth + 1, (int)v, true);
								if (depthToCache > 0) { d.Cache(dictOnly, depthToCache - 1); }
								mInstance.mStream.Position = cachedPos;
								value = d;
							}
							break;
					}
					if (!string.IsNullOrEmpty(key)) {
						mDatas.Add(key, Node.CreateNodeFromDB(this, key, valuePos, type, value));
						mKeys.Add(key);
					}
				}
			}

			public void ClearCache() {
				if (CachedState == eCacheState.None) { return; }
				foreach (KeyValuePair<string, Node> kv in mDatas) {
					CachedDatas cd = kv.Value.data as CachedDatas;
					if (cd == null) { continue; }
					cd.ClearCache();
				}
				mDatas.Clear();
				mKeys.Clear();
				for (int i = mSegs.Count - 1; i > 0; i--) {
					mSegs.RemoveAt(i);
				}
				if (mSegs.Count > 0) {
					mEndPos = mSegs[0] << mInstance.mSegmentShift;
				}
				mEmpties = 0;
				CachedState = eCacheState.None;
			}

			public bool Contains(string key, int startIndex) {
				if (string.IsNullOrEmpty(key)) { return false; }
				Visit();
				int i = key.IndexOf('.', startIndex);
				bool done = i < 0;
				Cache(!done, 0);
				string k = key.Substring(startIndex, (done ? key.Length : i) - startIndex);
				Node n;
				if (!mDatas.TryGetValue(k, out n)) { return false; }
				CachedDatas d = n.data as CachedDatas;
				if (!done) {
					if (n.type != eDataType.Dict) { return false; }
					if (d == null) { return false; }
					return d.Contains(key, i + 1);
				}
				return true;
			}

			public bool GetData(string key, int startIndex, out eDataType type, out object value) {
				type = eDataType.Empty;
				value = null;
				if (string.IsNullOrEmpty(key)) { return false; }
				Visit();
				int i = key.IndexOf('.', startIndex);
				bool done = i < 0;
				if (done) { Cache(false, 0); }
				string k = key.Substring(startIndex, (done ? key.Length : i) - startIndex);
				Node n;
				if (!mDatas.TryGetValue(k, out n)) { return false; }
				CachedDatas d = n.data as CachedDatas;
				if (!done) {
					if (n.type != eDataType.Dict) { return false; }
					if (d == null) { return false; }
					d.Cache(false, 0);
					return d.GetData(key, i + 1, out type, out value);
				}
				if (d != null) { d.Cache(false, -1); }
				type = n.type;
				value = n.data;
				return true;
			}

			public bool SetData(string key, int startIndex, eDataType type, object value) {
				if (string.IsNullOrEmpty(key)) { return false; }
				Visit();
				int i = key.IndexOf('.', startIndex);
				bool done = i < 0;
				string k = key.Substring(startIndex, (done ? key.Length : i) - startIndex);
				Node n;
				if (done) {
					Cache(false, -1);
					return SetKeyValue(k, type, value, out n);
				}
				if (!mDatas.TryGetValue(k, out n)) {
					SetKeyValue(k, eDataType.Dict, empty_dict, out n);
				}
				if (n == null) { return false; }
				CachedDatas cd = n.data as CachedDatas;
				if (cd == null) { return false; }
				cd.Cache(true, 0);
				return cd.SetData(key, i + 1, type, value);
			}

			private bool SetKeyValue(string key, eDataType type, object value, out Node n) {
				if (type == eDataType.JumpTo) { n = null; return false; }
				if (mDatas.TryGetValue(key, out n)) {
					if (type == eDataType.Empty) {
						BackupDatas();
						mDatas.Remove(key);
						mKeys.Remove(key);
					}
					n.SetData(type, value);
				} else if (type != eDataType.Empty) {
					n = Node.CreateNewNode(this, key);
					if (!n.SetData(type, value)) { return false; }
					BackupDatas();
					mDatas.Add(key, n);
					mKeys.Add(key);
				}
				if (n != null) { mInstance.ProcessEnqueue(n); }
				return true;
			}

			private static byte[] string_writer_buffer = new byte[512];
			private int WriteString(string str, int seg) {
				mInstance.mStream.Position = seg << mInstance.mSegmentShift;
				int sl = str.Length;
				int si = 0;
				int size = 0;
				int sizeInSeg = 0;
				while (true) {
					int sr = sl - si;
					bool finish = false;
					if (sr > 128) { sr = 128; } else { finish = true; }
					int bs = Encoding.UTF8.GetBytes(str, si, sr, string_writer_buffer, 0);
					size += bs;
					int bi = 0;
					while (true) {
						int segLeft = (1 << mInstance.mSegmentShift) - sizeInSeg - 4;
						if (bs - bi <= segLeft) { break; }
						mInstance.mStream.Write(string_writer_buffer, bi, segLeft);
						bi += segLeft;
						int nseg = mInstance.GetEmptySegment();
						string_writer_buffer[508] = (byte)(nseg >> 24);
						string_writer_buffer[509] = (byte)(nseg >> 16);
						string_writer_buffer[510] = (byte)(nseg >> 8);
						string_writer_buffer[511] = (byte)nseg;
						mInstance.mStream.Write(string_writer_buffer, 508, 4);
						mInstance.mStream.Position = (long)nseg << mInstance.mSegmentShift;
						sizeInSeg = 0;
					}
					mInstance.mStream.Write(string_writer_buffer, bi, bs - bi);
					if (finish) { break; }
					si += sr;
				}
				return size;
			}

			private string ReadString(int len, int seg) {
				byte[] buffer = mInstance.mBuffer.Get(len + 4);
				int blockSize = 1 << mInstance.mSegmentShift;
				mInstance.mStream.Position = seg << mInstance.mSegmentShift;
				int left = len;
				while (true) {
					bool done = left <= blockSize - 4;
					int toread = done ? left : (blockSize - 4);
					if (mInstance.mStream.Read(buffer, len - left + 4, toread) != toread) {
						throw new Exception("Invalid database");
					}
					if (done) { break; }
					left -= toread;
					if (mInstance.mStream.Read(buffer, 0, 4) != 4) {
						throw new Exception("Invalid database");
					}
					uint nseg = ((uint)buffer[0] << 24) | ((uint)buffer[1] << 16) | ((uint)buffer[2] << 8) | (uint)buffer[3];
					mInstance.mStream.Position = (long)nseg << mInstance.mSegmentShift;
				}
				return Encoding.UTF8.GetString(buffer, 4, len);
			}

			bool IDataReader.Reset() {
				if (CachedState != eCacheState.All) {
					lock (mInstance.mLocker) {
						Cache(false, 0);
					}
				}
				mItor = 0;
				Visit();
				return true;
			}

			bool IDataReader.Read(out string key, out eDataType type, out object value) {
				key = null;
				type = eDataType.Null;
				value = null;
				if (mItor < 0) { return false; }
				if (mItor >= mKeys.Count) {
					mItor = -1;
					Visit();
					return false;
				}
				key = mKeys[mItor++];
				lock (mInstance.mLocker) {
					Node n;
					if (mDatas.TryGetValue(key, out n)) {
						type = n.type;
						value = n.data;
					}
				}
				return true;
			}

			int IHelper.GetEmptySegment() { return mInstance.GetEmptySegment(); }

			CachedDatas IHelper.AddChildNode(Node node, IDataReader children) {
				if (children == null || !children.Reset()) { return null; }
				CachedDatas cd = new CachedDatas(mInstance, depth + 1, -1, false);
				string k;
				eDataType t;
				object v;
				while (children.Read(out k, out t, out v)) {
					Node n;
					cd.SetKeyValue(k, t, v, out n);
				}
				return cd;
			}

			void IHelper.Free(long pos, bool recursive) {
				mInstance.mStream.Position = pos;
				byte[] buffer = mInstance.mBuffer.Get(16);
				if (mInstance.mStream.Read(buffer, 0, 5) != 5) { throw new Exception("Invalid database"); }
				if (buffer[4] > 4) {
					mInstance.SetSegmentUnused((int)((((uint)buffer[0] & 0x0fu) << 24) | ((uint)buffer[1] << 16) | ((uint)buffer[2] << 8) | (uint)buffer[3]));
				}
				eDataType type = (eDataType)((uint)buffer[0] >> 4);
				if (type == eDataType.String) {
					if (mInstance.mStream.Read(buffer, 5, 6) != 6) { throw new Exception("Invalid database"); }
					int ks = (int)(((uint)buffer[5] << 12) | ((uint)buffer[6] << 4) | ((uint)buffer[7] >> 4));
					if (ks > 3) {
						int blockSize = 1 << mInstance.mSegmentShift;
						int seg = (int)((((uint)buffer[7] & 0x0fu) << 24) | ((uint)buffer[8] << 16) | ((uint)buffer[9] << 8) | (uint)buffer[10]);
						while (ks > 0) {
							int nseg = 0;
							if (ks > blockSize - 4) {
								mInstance.mStream.Position = (seg << mInstance.mSegmentShift) + blockSize - 4;
								nseg = (int)(((uint)buffer[0] << 24) | ((uint)buffer[1] << 16) | ((uint)buffer[2] << 8) | (uint)buffer[3]);
							}
							mInstance.SetSegmentUnused(seg);
							seg = nseg;
							ks -= blockSize - 4;
						}
					}
				} else if (type == eDataType.Dict && recursive) {
					if (mInstance.mStream.Read(buffer, 5, 4) != 4) { throw new Exception("Invalid database"); }
					int seg = (int)(((uint)buffer[5] << 24) | ((uint)buffer[6] << 16) | ((uint)buffer[7] << 8) | (uint)buffer[8]);
					long p = seg << mInstance.mSegmentShift;
					mInstance.mStream.Position = p;
					while (true) {
						if (mInstance.mStream.Read(buffer, 0, 1) != 1) { throw new Exception("Invalid database"); }
						eDataType t = (eDataType)((uint)buffer[0] >> 4);
						if (t == eDataType.Empty) {
							if (mInstance.mStream.Read(buffer, 1, 1) != 1) { throw new Exception("Invalid database"); }
							int es = buffer[1] & 0x0ff;
							if (es <= 0) { break; }
							if (mInstance.mStream.Read(buffer, 2, es - 2) != es - 2) { throw new Exception("Invalid database"); }
							continue;
						}
						int size = GetTypeSize(t);
						if (t == eDataType.JumpTo) {
							if (mInstance.mStream.Read(buffer, 1, 4) != 4) { throw new Exception("Invalid database"); }
							int nseg = (int)((((uint)buffer[0] & 0x0f) << 24) | ((uint)buffer[1] << 16) | ((uint)buffer[2] << 8) | (uint)buffer[3]);
							long npos = nseg << mInstance.mSegmentShift;
							mInstance.SetSegmentUnused(seg);
							mInstance.mStream.Position = npos;
							seg = nseg;
							continue;
						}
						long cpos = mInstance.mStream.Position - 1L;
						(this as IHelper).Free(cpos, recursive);
						mInstance.mStream.Position = cpos + size;
					}
					mInstance.SetSegmentUnused(seg);
				}
				mInstance.mStream.Position = pos;
				buffer[0] = (byte)((int)eDataType.Empty << 4);
				buffer[1] = (byte)GetTypeSize(type);
				mInstance.mStream.Write(buffer, 0, 2);
				mEmpties++;
			}

			long IHelper.GetExtendFromPos(Node node, int size) {
				CheckSegmentAndPosition();
				int endSeg = mSegs[mSegs.Count - 1];
				byte[] buffer = mInstance.mBuffer.Get(16);
				long blockSize = 1L << mInstance.mSegmentShift;
				while (true) {
					long posInBlock = mEndPos - (endSeg << mInstance.mSegmentShift);
					int typeJumpSize = GetTypeSize(eDataType.JumpTo);
					if (posInBlock + size + typeJumpSize > blockSize) {
						if (mEmpties > 3) {
							foreach (KeyValuePair<string, Node> kv in mDatas) {
								if (kv.Value == node) { continue; }
								kv.Value.Free();
							}
							for (int i = mSegs.Count - 1; i > 0; i--) {
								mInstance.SetSegmentUnused(mSegs[i]);
								mSegs.RemoveAt(i);
							}
							mEmpties = 0;
							mEndPos = mSegs[0] << mInstance.mSegmentShift;
							if (mEndPos == 0) {
								mEmpties = 1;
								mEndPos = 7;
							}
							for (int i = 0, imax = mKeys.Count; i < imax; i++) {
								Node n;
								if (!mDatas.TryGetValue(mKeys[i], out n)) { continue; }
								if (n == node) { continue; }
								if (n.ToProcess) { continue; }
								n.Write(true);
							}
							continue;
						}
						mInstance.mStream.Position = mEndPos;
						endSeg = mInstance.GetEmptySegment();
						mSegs.Add(endSeg);
						buffer[0] = (byte)(((int)eDataType.JumpTo << 4) | ((endSeg >> 24) & 0x0f));
						buffer[1] = (byte)(endSeg >> 16);
						buffer[2] = (byte)(endSeg >> 8);
						buffer[3] = (byte)endSeg;
						mInstance.mStream.Write(buffer, 0, 4);
						mEndPos = endSeg << mInstance.mSegmentShift;
					}
					break;
				}
				long end = mEndPos + size;
				mInstance.mStream.Position = end;
				buffer[0] = (byte)((int)eDataType.Empty << 4);
				buffer[1] = 0;
				mInstance.mStream.Write(buffer, 0, 2);
				long ret = mEndPos;
				mEndPos = end;
				return ret;
			}

			int IHelper.WriteString(string str, int seg) { return WriteString(str, seg); }

			private void CheckSegmentAndPosition() {
				if (mSegs.Count > 0) { return; }
				int seg = mInstance.GetEmptySegment();
				mSegs.Add(seg);
				mEndPos = (long)seg << mInstance.mSegmentShift;
			}

			private void BackupDatas() {
				if (mDatas_ != null) { return; }
				KeyValuePair<List<string>, Dictionary<string, Node>> ld = mInstance.mCachedNodeDatas.Count > 0 ? mInstance.mCachedNodeDatas.Dequeue() :
					new KeyValuePair<List<string>, Dictionary<string, Node>>(new List<string>(), new Dictionary<string, Node>());
				mKeys_ = ld.Key;
				mDatas_ = ld.Value;
				mKeys_.Clear();
				mDatas_.Clear();
				mKeys_.AddRange(mKeys);
				foreach (KeyValuePair<string, Node> kv in mDatas) {
					mDatas_.Add(kv.Key, kv.Value);
				}
				mInstance.mProcessQueue.Enqueue(this);
			}

			void IProcess.Flush() {
				if (mDatas_ != null && mKeys_ != null) {
					mKeys_.Clear();
					mDatas_.Clear();
					mInstance.mCachedNodeDatas.Enqueue(new KeyValuePair<List<string>, Dictionary<string, Node>>(mKeys_, mDatas_));
				}
				mDatas_ = null;
				mKeys_ = null;
			}

			void IProcess.Undo() {
				if (mDatas_ != null && mKeys_ != null) {
					List<string> keys = mKeys;
					Dictionary<string, Node> datas = mDatas;
					mKeys = mKeys_;
					mDatas = mDatas_;
					keys.Clear();
					datas.Clear();
					mInstance.mCachedNodeDatas.Enqueue(new KeyValuePair<List<string>, Dictionary<string, Node>>(keys, datas));
				}
				mDatas_ = null;
				mKeys_ = null;
			}

			private CachedDatas prev;
			private CachedDatas next;

			private static CachedDatas s_head;
			private static CachedDatas s_last;

			private static object locker = new object();

			private static void AddCachedDatas(CachedDatas cd) {
				if (cd == null) { return; }
				lock (locker) {
					if (s_head == null || s_last == null) {
						s_head = cd;
					} else {
						s_last.next = cd;
						cd.prev = s_last;
					}
					s_last = cd;
				}
			}

			private static void RemoveCachedDatas(CachedDatas cd) {
				if (cd == null) { return; }
				lock (locker) {
					CachedDatas prev = cd.prev;
					CachedDatas next = cd.next;
					cd.prev = null;
					cd.next = null;
					if (prev != null) { prev.next = next; }
					if (next != null) { next.prev = prev; }
					if (cd == s_head) { s_head = next; }
					if (cd == s_last) { s_last = prev; }
				}
			}

			public static void RemoveTimeouts() {
				DateTime now = DateTime.UtcNow;
				lock (locker) {
					while (s_head != null) {
						if (now <= s_head.Timeout) { break; }
						s_head.ClearCache();
					}
				}
			}

			private static bool SortCachedData(CachedDatas cd) {
				if (cd == null) { return false; }
				bool flag = false;
				CachedDatas prev = cd.prev;
				CachedDatas next = cd.next;
				lock (locker) {
					if (prev != null && next != null && prev.Timeout > next.Timeout) {
						Swap(prev, next);
						CachedDatas cc = prev;
						prev = next;
						next = cc;
					}
					if (prev != null && prev.Timeout > cd.Timeout) {
						Swap(prev, cd);
						flag = true;
					}
					if (next != null && next.Timeout < cd.Timeout) {
						Swap(next, cd);
						flag = true;
					}
				}
				return flag;
			}

			private static bool Swap(CachedDatas a, CachedDatas b) {
				if (a == null || b == null || a == b) { return false; }
				CachedDatas ap = a.prev;
				CachedDatas an = a.next;
				CachedDatas bp = b.prev;
				CachedDatas bn = b.next;
				if (ap != null) { ap.next = b; }
				if (an != null) { an.prev = b; }
				if (bp != null) { bp.next = a; }
				if (bn != null) { bn.prev = a; }
				a.prev = a == bp ? b : bp;
				a.next = a == bn ? b : bn;
				b.prev = b == ap ? a : ap;
				b.next = b == an ? a : an;
				if (a == s_head) { s_head = b; }
				if (a == s_last) { s_last = b; }
				if (b == s_head) { s_head = a; }
				if (b == s_last) { s_last = a; }
				return true;
			}
		}

		private class EmptyDict : IDataReader {
			public bool Read(out string key, out eDataType type, out object value) {
				key = null;
				type = eDataType.Empty;
				value = null;
				return false;
			}
			public bool Reset() {
				return true;
			}
		}
		private static IDataReader empty_dict = new EmptyDict();

		private Queue<KeyValuePair<List<string>, Dictionary<string, Node>>> mCachedNodeDatas = new Queue<KeyValuePair<List<string>, Dictionary<string, Node>>>(8);

	}

}
