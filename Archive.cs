using System;
using System.Collections;
using System.Collections.Generic;
using System.Reflection;
using System.Threading;
using GreatClock.Common.RTS.Collections;
using GreatClock.Common.RTS.DB;
using GreatClock.Common.RTS.Utilities;

namespace GreatClock.Common.RTS {

	public static class Archive {

		private const bool DEFAULT_AUTO_SYNC = true;

		private static KVDB data_base = null;

		#region interfaces

		public interface IArchiveVersionHelper {
			int version { get; }
			void MoveDataNodes(string name, string pathFrom, string pathTo);
			void MoveSheetNodes(string sheet, string pathFrom, string pathTo);
			void ModifyDataValue(string name, string path, DataTransfer transfer);
			void ModifySheetValue(string sheet, string path, DataTransfer transfer);
			bool FlushAll();
			void SetVersion(int version);
		}

		public delegate bool DataTransfer(ref eDataType type, ref object value);

		public delegate int ArchiveVersionMigration(IArchiveVersionHelper helper);

		public static void Init(string path, ArchiveVersionMigration versionMigrater, IList<string> errors) {
			if (data_base != null) { return; }
			data_base = new KVDB(6, path);
			if (versionMigrater != null) {
				int version = 0;
				eDataType t;
				object v;
				if (data_base.GetData("ver", out t, out v) && t == eDataType.Int) {
					version = (int)v;
				}
				IArchiveVersionHelper helper = new VersionHelper(version, errors);
				int nv = versionMigrater(helper);
				helper.SetVersion(nv);
				if (!helper.FlushAll()) {
					UnityEngine.Debug.LogError("Failed to migrate data ...");
				}
			}
		}

		public static T GetData<T>(string name, bool createIfMissing) where T : DataNodeBase, new() {
			if (data_base == null) { throw new InvalidOperationException("Archive Not Inited"); }
			InitDataContainer(createIfMissing);
			return data_container.Get<T>(name, createIfMissing);
		}

		public static int GetAllDataNames(IList<string> names) {
			if (data_base == null) { throw new InvalidOperationException("Archive Not Inited"); }
			eDataType t;
			object v;
			if (!data_base.GetData("cfg", out t, out v)) { return -1; }
			IDataReader reader = v as IDataReader;
			if (reader == null || !reader.Reset()) { return -1; }
			int n = 0;
			string key;
			while (reader.Read(out key, out t, out v)) {
				if (key == "#") { continue; }
				if (names != null) { names.Add(key); }
				n++;
			}
			return n;
		}

		public static bool RemoveData(string name) {
			if (data_base == null) { throw new InvalidOperationException("Archive Not Inited"); }
			InitDataContainer(false);
			return data_container.Remove(name);
		}

		public static bool RemoveDataFromCache(string name) {
			if (data_base == null) { throw new InvalidOperationException("Archive Not Inited"); }
			InitDataContainer(false);
			return data_container.RemoveCache(name, true);
		}

		public static T GetDataInSheet<T>(string sheet, int id, bool createIfMissing) where T : DataNodeBase, new() {
			if (data_base == null) { throw new InvalidOperationException("Archive Not Inited"); }
			ObjectContainerInt container = int_tables.Get(sheet, createIfMissing);
			return container == null ? null : container.Get<T>(id, createIfMissing);
		}

		public static T GetDataInSheet<T>(string sheet, string key, bool createIfMissing) where T : DataNodeBase, new() {
			if (data_base == null) { throw new InvalidOperationException("Archive Not Inited"); }
			ObjectContainerString container = string_tables.Get(sheet, createIfMissing);
			return container == null ? null : container.Get<T>(key, createIfMissing);
		}

		public static int GetAllIdsInSheet(string sheet, IList<int> ids) {
			if (string.IsNullOrEmpty(sheet)) { return -1; }
			if (data_base == null) { throw new InvalidOperationException("Archive Not Inited"); }
			eDataType t;
			object v;
			if (!data_base.GetData(sheet + ".#", out t, out v) || t != eDataType.String) { return -1; }
			eDataCategory cat;
			if (!Enum.TryParse<eDataCategory>(v as string, out cat) || cat != eDataCategory.id) { return -1; }
			if (!data_base.GetData(sheet, out t, out v)) { return -1; }
			IDataReader reader = v as IDataReader;
			if (reader == null || !reader.Reset()) { return -1; }
			int n = 0;
			string key;
			while (reader.Read(out key, out t, out v)) {
				if (key == "#") { continue; }
				if (ids != null) {
					int id;
					if (DecompressId(key, out id)) {
						ids.Add(id);
					}
				}
				n++;
			}
			return n;
		}

		public static int GetAllKeysInSheet(string sheet, IList<string> keys) {
			if (string.IsNullOrEmpty(sheet)) { return -1; }
			if (data_base == null) { throw new InvalidOperationException("Archive Not Inited"); }
			eDataType t;
			object v;
			if (!data_base.GetData(sheet + ".#", out t, out v) || t != eDataType.String) { return -1; }
			eDataCategory cat;
			if (!Enum.TryParse<eDataCategory>(v as string, out cat) || cat != eDataCategory.str) { return -1; }
			if (!data_base.GetData(sheet, out t, out v)) { return -1; }
			IDataReader reader = v as IDataReader;
			if (reader == null || !reader.Reset()) { return -1; }
			int n = 0;
			string key;
			while (reader.Read(out key, out t, out v)) {
				if (key == "#") { continue; }
				if (keys != null) { keys.Add(key); }
				n++;
			}
			return n;
		}

		public static bool RemoveDataInSheet(string sheet, int id) {
			if (data_base == null) { throw new InvalidOperationException("Archive Not Inited"); }
			ObjectContainerInt container = int_tables.Get(sheet, false);
			return container == null ? false : container.Remove(id);
		}

		public static bool RemoveDataInSheet(string sheet, string key) {
			if (data_base == null) { throw new InvalidOperationException("Archive Not Inited"); }
			ObjectContainerString container = string_tables.Get(sheet, false);
			return container == null ? false : container.Remove(key);
		}

		public static bool RemoveDataInSheetFromCache(string sheet, int id) {
			if (data_base == null) { throw new InvalidOperationException("Archive Not Inited"); }
			ObjectContainerInt container = int_tables.Get(sheet, false);
			return container == null ? false : container.RemoveCache(id, true);
		}

		public static bool RemoveDataInSheetFromCache(string sheet, string key) {
			if (data_base == null) { throw new InvalidOperationException("Archive Not Inited"); }
			ObjectContainerString container = string_tables.Get(sheet, false);
			return container == null ? false : container.RemoveCache(key, true);
		}

		public static bool GetDataAutoSync(string name) {
			if (data_base == null) { throw new InvalidOperationException("Archive Not Inited"); }
			InitDataContainer(false);
			return data_container.GetAutoSync(name);
		}

		public static bool SetDataAutoSync(string name, bool autoSync) {
			if (data_base == null) { throw new InvalidOperationException("Archive Not Inited"); }
			InitDataContainer(false);
			return data_container.SetAutoSync(name, autoSync);
		}

		public static bool GetSheetAutoSync(string sheet) {
			if (data_base == null) { throw new InvalidOperationException("Archive Not Inited"); }
			ObjectContainerInt tableInt = int_tables.Get(sheet, false);
			if (tableInt != null) { return tableInt.AutoSync; }
			ObjectContainerString tableString = string_tables.Get(sheet, false);
			if (tableString != null) { return tableString.AutoSync; }
			return false;
		}

		public static bool SetSheetAutoSync(string sheet, bool autoSync) {
			if (data_base == null) { throw new InvalidOperationException("Archive Not Inited"); }
			bool ret = false;
			ObjectContainerInt tableInt = int_tables.Get(sheet, false);
			if (tableInt != null) { tableInt.AutoSync = autoSync; ret = true; }
			ObjectContainerString tableString = string_tables.Get(sheet, false);
			if (tableString != null) { tableString.AutoSync = autoSync; ret = true; }
			return ret;
		}

		public static void SyncData(DataNodeBase data) {
			if (data_base == null) { throw new InvalidOperationException("Archive Not Inited"); }
			DataInstanceMark mark;
			if (!managed_datas.TryGetValue(data, out mark)) { return; }
			ThreadPool.QueueUserWorkItem((object obj) => {
				SyncDataToDB(data);
			});
		}

		public static void Close() {
			sync_batches.Stop();
			if (data_base != null) { data_base.Dispose(); data_base = null; }
		}

		#endregion

		#region migration
		private class VersionHelper : IArchiveVersionHelper {
			private List<KVDB.DataItem> mDataItems = new List<KVDB.DataItem>(1024);
			private List<string> mTempKeys = new List<string>(256);
			private IList<string> mErrors;
			public VersionHelper(int version, IList<string> errors) {
				this.version = version;
				mErrors = errors;
			}
			public int version { get; private set; }
			public void ModifyDataValue(string name, string path, DataTransfer transfer) {
				if (string.IsNullOrEmpty(name) || string.IsNullOrEmpty(path)) { return; }
				eDataType t;
				object v;
				string key = "cfg." + name + ".v." + path;
				if (!data_base.GetData(key, out t, out v)) { return; }
				if (!transfer(ref t, ref v)) { return; }
				mDataItems.Add(new KVDB.DataItem(key, t, v));
			}
			public void ModifySheetValue(string sheet, string path, DataTransfer transfer) {
				if (string.IsNullOrEmpty(sheet) || string.IsNullOrEmpty(path)) { return; }
				eDataType t;
				object v;
				if (!data_base.GetData(sheet, out t, out v)) { return; }
				IDataReader reader = v as IDataReader;
				if (reader == null || !reader.Reset()) { return; }
				mTempKeys.Clear();
				string k;
				while (reader.Read(out k, out t, out v)) {
					if (k == "#") { continue; }
					mTempKeys.Add(k);
				}
				for (int i = 0, imax = mTempKeys.Count; i < imax; i++) {
					string key = sheet + "." + mTempKeys[i] + ".v." + path;
					if (!data_base.GetData(key, out t, out v)) { continue; }
					if (!transfer(ref t, ref v)) { continue; }
					mDataItems.Add(new KVDB.DataItem(key, t, v));
				}
				mTempKeys.Clear();
			}
			public void MoveDataNodes(string name, string pathFrom, string pathTo) {
				if (string.IsNullOrEmpty(name) || string.IsNullOrEmpty(pathFrom)) { return; }
				string to = string.IsNullOrEmpty(pathTo) ? null : ("cfg." + name + ".v." + pathTo);
				Move("cfg." + name + ".v." + pathFrom, to);
			}
			public void MoveSheetNodes(string sheet, string pathFrom, string pathTo) {
				if (string.IsNullOrEmpty(sheet) || string.IsNullOrEmpty(pathFrom)) { return; }
				eDataType t;
				object v;
				if (!data_base.GetData(sheet, out t, out v)) { return; }
				IDataReader reader = v as IDataReader;
				if (reader == null || !reader.Reset()) { return; }
				mTempKeys.Clear();
				string k;
				while (reader.Read(out k, out t, out v)) {
					if (k == "#") { continue; }
					mTempKeys.Add(k);
				}
				for (int i = 0, imax = mTempKeys.Count; i < imax; i++) {
					string key = sheet + "." + mTempKeys[i] + ".v.";
					string to = string.IsNullOrEmpty(pathTo) ? null : (key + pathTo);
					Move(key + pathFrom, to);
				}
				mTempKeys.Clear();
			}
			public void SetVersion(int version) {
				if (this.version == version) { return; }
				this.version = version;
				mDataItems.Add(new KVDB.DataItem("ver", eDataType.Int, version));
			}
			public bool FlushAll() {
				foreach (KVDB.DataItem item in mDataItems) {
					UnityEngine.Debug.Log($"key:{item.key}, type:{item.type}, value:{item.value}");
				}
				int failedAt;
				if (data_base.SetData(mDataItems, out failedAt)) {
					mDataItems.Clear();
					return true;
				}
				if (mErrors != null) {
					KVDB.DataItem failedItem = mDataItems[failedAt];
					mErrors.Add($"Failed in SetValue for key:{failedItem.key}, type:{failedItem.type}");
				}
				return false;
			}
			private void Move(string from, string to) {
				mDataItems.Add(new KVDB.DataItem(from, eDataType.Empty, null));
				if (string.IsNullOrEmpty(to)) { return; }
				eDataType t;
				object v;
				if (!data_base.GetData(from, out t, out v)) { return; }
				if (t == eDataType.Dict) {
					v = TempData.CreateFromDataReader(v as IDataReader);
				}
				mDataItems.Add(new KVDB.DataItem(to, t, v));
			}
		}

		private struct TempDataItem {
			public string key;
			public eDataType type;
			public object value;
			public TempDataItem(string key, eDataType type, object value) {
				this.key = key;
				this.type = type;
				this.value = value;
			}
		}

		private class TempData : IDataReader {
			private List<TempDataItem> mDatas = new List<TempDataItem>();
			private int mIndex = -1;
			private TempData() { }
			public static TempData CreateFromDataReader(IDataReader reader) {
				if (reader == null || !reader.Reset()) { return null; }
				TempData d = new TempData();
				string k;
				eDataType t;
				object v;
				while (reader.Read(out k, out t, out v)) {
					if (t == eDataType.Dict) {
						v = CreateFromDataReader(v as IDataReader);
					}
					d.mDatas.Add(new TempDataItem(k, t, v));
				}
				return d;
			}
			public bool Reset() {
				mIndex = 0;
				return true;
			}
			public bool Read(out string key, out eDataType type, out object value) {
				if (mIndex < 0 || mIndex >= mDatas.Count) {
					key = null;
					type = eDataType.Empty;
					value = null;
					return false;
				}
				TempDataItem d = mDatas[mIndex];
				mIndex++;
				key = d.key;
				type = d.type;
				value = d.value;
				return true;
			}
		}
		#endregion

		#region managed data container

		private abstract class ObjectContainer<T> {
			public readonly eDataCategory category;
			public readonly string table;
			// this 'AutoSync' is used for table only
			public bool AutoSync = DEFAULT_AUTO_SYNC;
			private Dictionary<T, KeyValuePair<DataNodeBase, bool>> mCachedItems = new Dictionary<T, KeyValuePair<DataNodeBase, bool>>(64);
			protected ObjectContainer(eDataCategory category, string table) {
				this.category = category;
				this.table = table;
			}
			public V Get<V>(T id, bool createIfMissing) where V : DataNodeBase, new() {
				KeyValuePair<DataNodeBase, bool> kv;
				if (!mCachedItems.TryGetValue(id, out kv) || kv.Key == null) {
					DataNodeBase data;
					GetDataInDBTable(id, out data);
					if (data == null && createIfMissing) {
						data = new V();
					}
					if (data != null) {
						data.onModify = s_on_data_modify;
						kv = new KeyValuePair<DataNodeBase, bool>(data, DEFAULT_AUTO_SYNC);
						mCachedItems.Remove(id);
						mCachedItems.Add(id, kv);
						AddManagedData(data, GetDataInstanceMark(id));
					}
				}
				return kv.Key as V;
			}
			public bool GetAutoSync(T id) {
				KeyValuePair<DataNodeBase, bool> kv;
				return mCachedItems.TryGetValue(id, out kv) ? kv.Value : DEFAULT_AUTO_SYNC;
			}
			public bool SetAutoSync(T id, bool autoSync) {
				KeyValuePair<DataNodeBase, bool> kv;
				if (!mCachedItems.TryGetValue(id, out kv)) {
					if (!autoSync) { return false; }
					kv = new KeyValuePair<DataNodeBase, bool>(null, autoSync);
					mCachedItems.Add(id, kv);
					return true;
				}
				if (kv.Value == autoSync) { return false; }
				mCachedItems.Remove(id);
				mCachedItems.Add(id, new KeyValuePair<DataNodeBase, bool>(kv.Key, autoSync));
				return true;
			}
			public bool Remove(T id) {
				KeyValuePair<DataNodeBase, bool> data;
				if (mCachedItems.TryGetValue(id, out data)) {
					mCachedItems.Remove(id);
					RemoveManagedData(data.Key, false, true);
				}
				return DataBaseRemove(id);
			}
			public bool RemoveCache(T id, bool clear) {
				KeyValuePair<DataNodeBase, bool> data;
				if (mCachedItems.TryGetValue(id, out data)) {
					mCachedItems.Remove(id);
					if (clear) { RemoveManagedData(data.Key, false, false); }
					return true;
				}
				return false;
			}
			protected abstract bool GetDataInDBTable(T id, out DataNodeBase value);
			protected abstract DataInstanceMark GetDataInstanceMark(T id);
			protected abstract bool DataBaseRemove(T id);
		}

		private class ObjectContainerInt : ObjectContainer<int> {
			public ObjectContainerInt(eDataCategory category, string table) : base(category, table) { }
			protected override bool GetDataInDBTable(int id, out DataNodeBase data) {
				data = null;
				eDataType t;
				object v;
				if (!data_base.GetData(table + "." + CompressId(id), out t, out v)) { return false; }
				IDataReader reader = v as IDataReader;
				reader.Reset();
				string key;
				Type type = null;
				while (reader.Read(out key, out t, out v)) {
					if (key == "t") {
						type = Type.GetType(v as string);
					} else if (key == "v") {
						if (type != null) {
							data = ParseFromDB(type, t, v) as DataNodeBase;
						}
					}
				}
				return true;
			}
			protected override DataInstanceMark GetDataInstanceMark(int id) {
				return new DataInstanceMark(category, table, CompressId(id), id);
			}
			protected override bool DataBaseRemove(int id) {
				string k = table + "." + CompressId(id);
				return data_base.Contains(k) && data_base.DeleteData(k);
			}
		}

		private class ObjectContainerString : ObjectContainer<string> {
			public ObjectContainerString(eDataCategory category, string table) : base(category, table) { }
			protected override bool GetDataInDBTable(string id, out DataNodeBase data) {
				data = null;
				eDataType t;
				object v;
				if (!data_base.GetData(table + "." + id, out t, out v)) { return false; }
				IDataReader reader = v as IDataReader;
				reader.Reset();
				string key;
				Type type = null;
				while (reader.Read(out key, out t, out v)) {
					if (key == "t") {
						type = Type.GetType(v as string);
					} else if (key == "v") {
						if (type != null) {
							data = ParseFromDB(type, t, v) as DataNodeBase;
						}
					}
				}
				return true;
			}
			protected override DataInstanceMark GetDataInstanceMark(string id) {
				return new DataInstanceMark(category, table, id, 0);
			}
			protected override bool DataBaseRemove(string id) {
				string k = table + "." + id;
				return data_base.Contains(k) && data_base.DeleteData(k);
			}
		}

		private abstract class TableContainer<T> where T : class {
			public readonly eDataCategory category;
			private readonly string mCategory;
			private Dictionary<string, T> mDict = new Dictionary<string, T>();
			public TableContainer(eDataCategory category) { this.category = category; mCategory = category.ToString(); }
			public T Get(string table, bool createIfMissing) {
				if (string.IsNullOrEmpty(table)) { return default(T); }
				T value;
				if (mDict.TryGetValue(table, out value)) { return value; }
				eDataType t;
				object v;
				if (data_base.GetData(table + ".#", out t, out v)) {
					if ((v as string) != mCategory) { return null; }
				} else {
					if (!createIfMissing) { return default(T); }
					if (!data_base.SetData(table + ".#", eDataType.String, mCategory)) { return default(T); }
				}
				value = CreateNew(table);
				mDict.Add(table, value);
				return value;
			}
			protected abstract T CreateNew(string table);
		}

		private class IntTableContainer : TableContainer<ObjectContainerInt> {
			public IntTableContainer(eDataCategory category) : base(category) { }
			protected override ObjectContainerInt CreateNew(string table) {
				return new ObjectContainerInt(category, table);
			}
		}

		private class StringTableContainer : TableContainer<ObjectContainerString> {
			public StringTableContainer(eDataCategory category) : base(category) { }
			protected override ObjectContainerString CreateNew(string table) {
				return new ObjectContainerString(category, table);
			}
		}

		private static ObjectContainerString data_container = null;

		private static IntTableContainer int_tables = new IntTableContainer(eDataCategory.id);
		private static StringTableContainer string_tables = new StringTableContainer(eDataCategory.str);

		private static void InitDataContainer(bool createIfMissing) {
			if (data_container != null) { return; }
			data_container = new ObjectContainerString(eDataCategory.cfg, "cfg");
			if (createIfMissing && !data_base.Contains(data_container.table)) {
				data_base.SetData(data_container.table + ".#", eDataType.String, "cfg");
			}
		}

		#endregion

		#region user data management

		private enum eDataCategory { cfg, id, str }

		private struct DataInstanceMark {
			public eDataCategory category;
			public string table;
			public int id;
			public string key;
			public DataInstanceMark(eDataCategory category, string table, string key, int id) {
				this.category = category;
				this.table = table;
				this.key = key;
				this.id = id;
			}
		}

		private static Dictionary<DataNodeBase, DataInstanceMark> managed_datas = new Dictionary<DataNodeBase, DataInstanceMark>(256);

		private static void AddManagedData(DataNodeBase data, DataInstanceMark mark) {
			if (data == null) { return; }
			data.onModify = s_on_data_modify;
			managed_datas.Add(data, mark);
		}

		private static void RemoveManagedData(DataNodeBase data, bool clear, bool isDelete) {
			if (data == null) { return; }
			data.onModify = null;
			DataInstanceMark mark;
			if (!managed_datas.TryGetValue(data, out mark)) { return; }
			managed_datas.Remove(data);
			if (isDelete) {
				sync_batches.Remove(data);
			}
			if (!clear) { return; }
			if (mark.table == null) {
				data_container.RemoveCache(mark.key, false);
			} else if (mark.key == null) {
				ObjectContainerInt inttable = int_tables.Get(mark.table, false);
				if (inttable != null) { inttable.RemoveCache(mark.id, false); }
			} else {
				ObjectContainerString stringtable = string_tables.Get(mark.table, false);
				if (stringtable != null) { stringtable.RemoveCache(mark.key, false); }
			}
		}

		#endregion

		#region sync to db

		private static OnModifyDelegate s_on_data_modify = OnDataModify;
		private static void OnDataModify(DataNodeBase data) {
			DataInstanceMark mark;
			if (!managed_datas.TryGetValue(data, out mark)) { return; }
			bool sync = false;
			switch (mark.category) {
				case eDataCategory.cfg:
					sync = data_container.GetAutoSync(mark.key);
					break;
				case eDataCategory.id:
					ObjectContainerInt intTable = int_tables.Get(mark.table, false);
					if (intTable != null) { sync = intTable.AutoSync; }
					break;
				case eDataCategory.str:
					ObjectContainerString stringTable = string_tables.Get(mark.table, false);
					if (stringTable != null) { sync = stringTable.AutoSync; }
					break;
			}
			if (sync) { sync_batches.Enqueue(data); }
		}

		private static Dictionary<Type, string> type_strings = new Dictionary<Type, string>(16);
		private static string GetTypeString(Type type) {
			if (type == null) { return null; }
			string ret;
			if (!type_strings.TryGetValue(type, out ret)) {
				ret = type.FullName + "," + type.Assembly.GetName().Name;
				type_strings.Add(type, ret);
			}
			return ret;
		}

		private static BatchAction<DataNodeBase> sync_batches = new BatchAction<DataNodeBase>(10, SyncDataToDB);

		private static List<DataLog> cached_logs = new List<DataLog>(64);

		private static void SyncDataToDB(DataNodeBase data) {
			DataInstanceMark mark;
			if (!managed_datas.TryGetValue(data, out mark)) { return; }
			string ts = GetTypeString(data.GetType());
			string kt = mark.table + "." + mark.key + ".t";
			// TODO if (data_base. kt != kt) { set all } else { set changes }
			eDataType t;
			object v;
			if (!data_base.GetData(kt, out t, out v) || t != eDataType.String || (v as string) != ts) {
				data_base.SetData(kt, eDataType.String, ts);
				data_base.SetData(mark.table + "." + mark.key + ".v", eDataType.Dict, new NodeReader(data));
				return;
			}
			cached_logs.Clear();
			(data as IDataLoggable).CollectChangeLogs(mark.table + "." + mark.key + ".v", cached_logs);
			(data as IDataLoggable).Reset();
			for (int i = 0, imax = cached_logs.Count; i < imax; i++) {
				DataLog log = cached_logs[i];
				if (log.op == '-') {
					data_base.DeleteData(log.path);
				} else {
					v = log.value;
					t = v == null ? eDataType.Null : GetDBDataType(v.GetType());
					if (t == eDataType.Dict) { v = GetDataReader(v); }
					data_base.SetData(log.path, t, v);
				}
			}
		}

		#endregion

		#region deserialize

		private static Type type_data_node_base = typeof(DataNodeBase);
		private static Type type_rts_list_interface = typeof(IRTSList);
		private static Type type_rts_dict_interface = typeof(IRTSDict);

		private static Type type_byte = typeof(byte);
		private static Type type_sbyte = typeof(sbyte);
		private static Type type_short = typeof(short);
		private static Type type_ushort = typeof(ushort);
		private static Type type_int = typeof(int);
		private static Type type_uint = typeof(uint);
		private static Type type_long = typeof(long);
		private static Type type_ulong = typeof(ulong);
		private static Type type_float = typeof(float);
		private static Type type_double = typeof(double);
		private static Type type_bool = typeof(bool);
		private static Type type_string = typeof(string);

		private static object ParseFromDB(Type datatype, eDataType type, object dbValue) {
			switch (type) {
				case eDataType.Byte: return ParseUnsignedInteger(datatype, (byte)dbValue);
				case eDataType.SByte: return ParseSignedInteger(datatype, (sbyte)dbValue);
				case eDataType.UShort: return ParseUnsignedInteger(datatype, (ushort)dbValue);
				case eDataType.Short: return ParseSignedInteger(datatype, (short)dbValue);
				case eDataType.UInt: return ParseUnsignedInteger(datatype, (uint)dbValue);
				case eDataType.Int: return ParseSignedInteger(datatype, (int)dbValue);
				case eDataType.ULong: return ParseUnsignedInteger(datatype, (ulong)dbValue);
				case eDataType.Long: return ParseSignedInteger(datatype, (long)dbValue);
				case eDataType.Float: return ParseFloat(datatype, (float)dbValue);
				case eDataType.Double: return ParseDouble(datatype, (double)dbValue);
				case eDataType.Bool:
				case eDataType.String:
					return dbValue;
				case eDataType.Dict:
					if (type_rts_list_interface.IsAssignableFrom(datatype)) {
						IRTSList list = Activator.CreateInstance(datatype) as IRTSList;
						return ParseArrayFromDB(list, datatype.GetGenericArguments()[0], dbValue as IDataReader) ? list : null;
					}
					if (type_rts_dict_interface.IsAssignableFrom(datatype)) {
						IRTSDict dict = Activator.CreateInstance(datatype) as IRTSDict;
						return ParseDictFromDB(dict, datatype.GetGenericArguments()[0], dbValue as IDataReader) ? dict : null;
					}
					return ParseNodeFromDB(datatype, dbValue as IDataReader);
			}
			return null;
		}

		private static object ParseSignedInteger(Type type, long integer) {
			if (type.Equals(type_byte)) { return (byte)integer; }
			if (type.Equals(type_sbyte)) { return (sbyte)integer; }
			if (type.Equals(type_ushort)) { return (ushort)integer; }
			if (type.Equals(type_short)) { return (short)integer; }
			if (type.Equals(type_uint)) { return (uint)integer; }
			if (type.Equals(type_int)) { return (int)integer; }
			if (type.Equals(type_ulong)) { return (ulong)integer; }
			if (type.Equals(type_long)) { return integer; }
			if (type.Equals(type_float)) { return (float)integer; }
			if (type.Equals(type_double)) { return (double)integer; }
			return null;
		}

		private static object ParseUnsignedInteger(Type type, ulong integer) {
			if (type.Equals(type_byte)) { return (byte)integer; }
			if (type.Equals(type_sbyte)) { return (sbyte)integer; }
			if (type.Equals(type_ushort)) { return (ushort)integer; }
			if (type.Equals(type_short)) { return (short)integer; }
			if (type.Equals(type_uint)) { return (uint)integer; }
			if (type.Equals(type_int)) { return (int)integer; }
			if (type.Equals(type_ulong)) { return integer; }
			if (type.Equals(type_long)) { return (long)integer; }
			if (type.Equals(type_float)) { return (float)integer; }
			if (type.Equals(type_double)) { return (double)integer; }
			return null;
		}

		private static object ParseFloat(Type type, float value) {
			if (type.Equals(type_byte)) { return (byte)value; }
			if (type.Equals(type_sbyte)) { return (sbyte)value; }
			if (type.Equals(type_ushort)) { return (ushort)value; }
			if (type.Equals(type_short)) { return (short)value; }
			if (type.Equals(type_uint)) { return (uint)value; }
			if (type.Equals(type_int)) { return (int)value; }
			if (type.Equals(type_ulong)) { return (ulong)value; }
			if (type.Equals(type_long)) { return (long)value; }
			if (type.Equals(type_float)) { return value; }
			if (type.Equals(type_double)) { return (double)value; }
			return null;
		}

		private static object ParseDouble(Type type, double value) {
			if (type.Equals(type_byte)) { return (byte)value; }
			if (type.Equals(type_sbyte)) { return (sbyte)value; }
			if (type.Equals(type_ushort)) { return (ushort)value; }
			if (type.Equals(type_short)) { return (short)value; }
			if (type.Equals(type_uint)) { return (uint)value; }
			if (type.Equals(type_int)) { return (int)value; }
			if (type.Equals(type_ulong)) { return (ulong)value; }
			if (type.Equals(type_long)) { return (long)value; }
			if (type.Equals(type_float)) { return (float)value; }
			if (type.Equals(type_double)) { return value; }
			return null;
		}

		private static object ParseNodeFromDB(Type type, IDataReader reader) {
			if (reader == null || !reader.Reset()) { return null; }
			NodeTypeFieldData data = NodeTypeFieldData.GetNodeTypeFieldData(type);
			if (data == null) { return null; }
			object ret = Activator.CreateInstance(type);
			string k;
			eDataType t;
			object v;
			while (reader.Read(out k, out t, out v)) {
				NodeTypeFieldData.FieldData field = data.GetField(k);
				if (field.field == null) { continue; }
				object value = ParseFromDB(field.field.FieldType, t, v);
				PropertyInfo prop = field.property;
				if (prop != null) { prop.SetValue(ret, value); } else { field.field.SetValue(ret, value); }
			}
			return ret;
		}

		private static bool ParseArrayFromDB(IRTSList list, Type type, IDataReader reader) {
			if (list == null || reader == null || !reader.Reset()) { return false; }
			string k;
			eDataType t;
			object v;
			while (reader.Read(out k, out t, out v)) {
				object value = ParseFromDB(type, t, v);
				double p = RTSListUtil.FromKey(k);
				if (!double.IsNaN(p)) { list.RawAdd(p, value); } else { list.Add(value); }
			}
			return true;
		}

		private static bool ParseDictFromDB(IRTSDict dict, Type type, IDataReader reader) {
			if (dict == null || reader == null || !reader.Reset()) { return false; }
			string k;
			eDataType t;
			object v;
			while (reader.Read(out k, out t, out v)) {
				dict.Add(k, ParseFromDB(type, t, v));
			}
			return true;
		}

		#endregion

		#region serialize

		private class NodeReader : IDataReader {
			private DataNodeBase mNode;
			private NodeTypeFieldData.FieldData[] mFields;
			private int mIndex = -1;
			public NodeReader(DataNodeBase node) {
				mNode = node;
				mFields = NodeTypeFieldData.GetNodeTypeFieldData(node.GetType()).GetFields();
			}
			public bool Read(out string key, out eDataType type, out object value) {
				if (mIndex < 0 || mIndex >= mFields.Length) {
					key = null;
					type = eDataType.Empty;
					value = null;
					return false;
				}
				NodeTypeFieldData.FieldData fd = mFields[mIndex++];
				key = fd.name;
				type = GetDBDataType(fd.field.FieldType);
				value = fd.property != null ? fd.property.GetValue(mNode) : fd.field.GetValue(mNode);
				if (type == eDataType.Dict) { value = GetDataReader(value); }
				return true;
			}

			public bool Reset() {
				if (mFields == null) { return false; }
				mIndex = 0;
				return true;
			}
		}

		private class ListReader : IDataReader {
			private KVDB.DataItem[] mItems;
			private int mIndex = -1;
			public ListReader(IRTSList list) {
				eDataType type = GetDBDataType(list.GetType().GetGenericArguments()[0]);
				int n = list.Count;
				mItems = new KVDB.DataItem[n];
				int i = 0;
				foreach (object item in list) {
					mItems[i] = new KVDB.DataItem(RTSListUtil.ToKey(list.GetPriority(i)), type, item);
					i++;
				}
			}
			public bool Read(out string key, out eDataType type, out object value) {
				if (mIndex < 0 || mIndex >= mItems.Length) {
					key = null;
					type = eDataType.Empty;
					value = null;
					return false;
				}
				KVDB.DataItem item = mItems[mIndex++];
				key = item.key;
				type = item.type;
				value = item.value;
				if (value == null) {
					type = eDataType.Null;
				} else if (type == eDataType.Dict) {
					value = GetDataReader(value);
				}
				return true;
			}
			public bool Reset() {
				if (mItems == null) { return false; }
				mIndex = 0;
				return true;
			}
		}

		private class DictReader : IDataReader {
			private KVDB.DataItem[] mItems;
			private int mIndex = -1;
			public DictReader(IRTSDict dict) {
				eDataType type = GetDBDataType(dict.GetType().GetGenericArguments()[0]);
				int n = dict.Count;
				mItems = new KVDB.DataItem[n];
				int i = 0;
				IDictionaryEnumerator iter = dict.GetEnumerator();
				while (iter.MoveNext()) {
					mItems[i] = new KVDB.DataItem(iter.Key.ToString(), type, iter.Value);
					i++;
				}
			}
			public bool Read(out string key, out eDataType type, out object value) {
				if (mIndex < 0 || mIndex >= mItems.Length) {
					key = null;
					type = eDataType.Empty;
					value = null;
					return false;
				}
				KVDB.DataItem item = mItems[mIndex++];
				key = item.key;
				type = item.type;
				value = item.value;
				if (value == null) {
					type = eDataType.Null;
				} else if (type == eDataType.Dict) {
					value = new DictReader(value as IRTSDict);
				}
				return true;
			}

			public bool Reset() {
				if (mItems == null) { return false; }
				mIndex = 0;
				return true;
			}
		}

		private static IDataReader GetDataReader(object data) {
			DataNodeBase node = data as DataNodeBase;
			if (node != null) { return new NodeReader(node); }
			IRTSList list = data as IRTSList;
			if (list != null) { return new ListReader(list); }
			IRTSDict dict = data as IRTSDict;
			if (dict != null) { return new DictReader(dict); }
			return null;
		}

		private static eDataType GetDBDataType(Type type) {
			if (type.Equals(type_byte)) { return eDataType.Byte; }
			if (type.Equals(type_sbyte)) { return eDataType.SByte; }
			if (type.Equals(type_ushort)) { return eDataType.UShort; }
			if (type.Equals(type_short)) { return eDataType.Short; }
			if (type.Equals(type_uint)) { return eDataType.UInt; }
			if (type.Equals(type_int)) { return eDataType.Int; }
			if (type.Equals(type_ulong)) { return eDataType.ULong; }
			if (type.Equals(type_long)) { return eDataType.Long; }
			if (type.Equals(type_float)) { return eDataType.Float; }
			if (type.Equals(type_double)) { return eDataType.Double; }
			if (type.Equals(type_bool)) { return eDataType.Bool; }
			if (type.Equals(type_string)) { return eDataType.String; }
			if (type.IsSubclassOf(type_data_node_base)) { return eDataType.Dict; }
			if (type_rts_list_interface.IsAssignableFrom(type)) { return eDataType.Dict; }
			if (type_rts_dict_interface.IsAssignableFrom(type)) { return eDataType.Dict; }
			return eDataType.Null;
		}

		#endregion

		#region support

		private static string int_compress = "0123456789_abcdefghijklmnopqrstuvwxyz$ABCDEFGHIJKLMNOPQRSTUVWXYZ";
		private static char[] temp_chars = new char[6];

		private static string CompressId(int id) {
			for (int i = 0; i < 6; i++) {
				int v = id & 63;
				temp_chars[i] = id == 0 && i > 0 ? ' ' : int_compress[v];
				id >>= 6;
			}
			return string.Concat(temp_chars).Trim();
		}

		private static bool DecompressId(string str, out int id) {
			id = 0;
			for (int i = str.Length - 1; i >= 0; i--) {
				id <<= 6;
				int index = int_compress.IndexOf(str[i]);
				if (index < 0) { return false; }
				id |= index;
			}
			return true;
		}

		#endregion

	}

}
