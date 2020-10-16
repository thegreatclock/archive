using System;
using System.Collections;
using System.Collections.Generic;
using System.Runtime.ConstrainedExecution;
using System.Threading;

namespace GreatClock.Common.RTS.Collections {

	public interface IRTSDict : IDictionary, IDataLoggable {
		OnDictModifyDelegate onModify { get; set; }
	}

	public class RTSDict<T> : IDictionary<string, T>, ICollection<KeyValuePair<string, T>>, IEnumerable<KeyValuePair<string, T>>, IEnumerable, IDictionary, ICollection, IReadOnlyDictionary<string, T>, IReadOnlyCollection<KeyValuePair<string, T>>, IRTSDict {

		public OnDictModifyDelegate onModify { get; set; }

		private struct Entry {
			public int hashCode;
			public int next;
			public string key;
			public T value;
			public byte s;
		}

		public struct Enumerator : IEnumerator<KeyValuePair<string, T>>, IDisposable, IEnumerator, IDictionaryEnumerator {
			private RTSDict<T> dictionary;
			private int version;
			private int index;
			private KeyValuePair<string, T> current;
			private int getEnumeratorRetType;
			internal const int DictEntry = 1;
			internal const int KeyValuePair = 2;
			public KeyValuePair<string, T> Current { get { return current; } }
			object IEnumerator.Current {
				get {
					if (index == 0 || index == dictionary.Count + 1) {
						throw new InvalidOperationException("CantHappen");
					}
					if (getEnumeratorRetType == 1) {
						return new DictionaryEntry(current.Key, current.Value);
					}
					return new KeyValuePair<string, T>(current.Key, current.Value);
				}
			}
			DictionaryEntry IDictionaryEnumerator.Entry {
				get {
					if (index == 0 || index == dictionary.count + 1) {
						throw new InvalidOperationException("CantHappen");
					}
					return new DictionaryEntry(current.Key, current.Value);
				}
			}
			object IDictionaryEnumerator.Key {
				get {
					if (index == 0 || index == dictionary.count + 1) {
						throw new InvalidOperationException("CantHappen");
					}
					return current.Key;
				}
			}
			object IDictionaryEnumerator.Value {
				get {
					if (index == 0 || index == dictionary.count + 1) {
						throw new InvalidOperationException("CantHappen");
					}
					return current.Value;
				}
			}
			internal Enumerator(RTSDict<T> dictionary, int getEnumeratorRetType) {
				this.dictionary = dictionary;
				version = dictionary.version;
				index = 0;
				this.getEnumeratorRetType = getEnumeratorRetType;
				current = default(KeyValuePair<string, T>);
			}
			public bool MoveNext() {
				if (version != dictionary.version) {
					throw new InvalidOperationException("FailedVersion");
				}
				while ((uint)index < (uint)dictionary.count) {
					if (dictionary.entries[index].hashCode >= 0) {
						current = new KeyValuePair<string, T>(dictionary.entries[index].key, dictionary.entries[index].value);
						index++;
						return true;
					}
					index++;
				}
				index = dictionary.count + 1;
				current = default(KeyValuePair<string, T>);
				return false;
			}
			public void Dispose() { }
			void IEnumerator.Reset() {
				if (version != dictionary.version) {
					throw new InvalidOperationException("FailedVersion");
				}
				index = 0;
				current = default(KeyValuePair<string, T>);
			}
		}

		public sealed class KeyCollection : ICollection<string>, IEnumerable<string>, IEnumerable, ICollection, IReadOnlyCollection<string> {
			public struct Enumerator : IEnumerator<string>, IDisposable, IEnumerator {
				private RTSDict<T> dictionary;
				private int index;
				private int version;
				private string currentKey;
				public string Current { get { return currentKey; } }
				object IEnumerator.Current {
					get {
						if (index == 0 || index == dictionary.count + 1) {
							throw new InvalidOperationException("CantHappen");
						}
						return currentKey;
					}
				}
				internal Enumerator(RTSDict<T> dictionary) {
					this.dictionary = dictionary;
					version = dictionary.version;
					index = 0;
					currentKey = default(string);
				}
				public void Dispose() { }
				public bool MoveNext() {
					if (version != dictionary.version) {
						throw new InvalidOperationException("FailedVersion");
					}
					while ((uint)index < (uint)dictionary.count) {
						if (dictionary.entries[index].hashCode >= 0) {
							currentKey = dictionary.entries[index].key;
							index++;
							return true;
						}
						index++;
					}
					index = dictionary.count + 1;
					currentKey = default(string);
					return false;
				}
				void IEnumerator.Reset() {
					if (version != dictionary.version) {
						throw new InvalidOperationException("FailedVersion");
					}
					index = 0;
					currentKey = default(string);
				}
			}
			private RTSDict<T> dictionary;
			public int Count { get { return dictionary.Count; } }
			bool ICollection<string>.IsReadOnly { get { return true; } }
			bool ICollection.IsSynchronized { get { return false; } }
			object ICollection.SyncRoot { get { return ((ICollection)dictionary).SyncRoot; } }
			public KeyCollection(RTSDict<T> dictionary) {
				if (dictionary == null) {
					throw new ArgumentNullException("dictionary");
				}
				this.dictionary = dictionary;
			}
			public Enumerator GetEnumerator() {
				return new Enumerator(dictionary);
			}
			public void CopyTo(string[] array, int index) {
				if (array == null) {
					throw new ArgumentNullException("array");
				}
				if (index < 0 || index > array.Length) {
					throw new ArgumentOutOfRangeException("index", "NeedNonNegNum");
				}
				if (array.Length - index < dictionary.Count) {
					throw new ArgumentException("ArrayPlusOffTooSmall");
				}
				int count = dictionary.count;
				Entry[] entries = dictionary.entries;
				for (int i = 0; i < count; i++) {
					if (entries[i].hashCode >= 0) {
						array[index++] = entries[i].key;
					}
				}
			}
			void ICollection<string>.Add(string item) {
				throw new NotSupportedException("KeyCollectionSet");
			}
			void ICollection<string>.Clear() {
				throw new NotSupportedException("KeyCollectionSet");
			}
			bool ICollection<string>.Contains(string item) {
				return dictionary.ContainsKey(item);
			}
			bool ICollection<string>.Remove(string item) {
				throw new NotSupportedException("KeyCollectionSet");
			}
			IEnumerator<string> IEnumerable<string>.GetEnumerator() {
				return new Enumerator(dictionary);
			}
			IEnumerator IEnumerable.GetEnumerator() {
				return new Enumerator(dictionary);
			}
			void ICollection.CopyTo(Array array, int index) {
				if (array == null) {
					throw new ArgumentNullException("array");
				}
				if (array.Rank != 1) {
					throw new ArgumentException("RankMultiDimNotSupported");
				}
				if (array.GetLowerBound(0) != 0) {
					throw new ArgumentException("NonZeroLowerBound");
				}
				if (index < 0 || index > array.Length) {
					throw new ArgumentOutOfRangeException("index", "NeedNonNegNum");
				}
				if (array.Length - index < dictionary.Count) {
					throw new ArgumentException("ArrayPlusOffTooSmall");
				}
				string[] array2 = array as string[];
				if (array2 != null) {
					CopyTo(array2, index);
					return;
				}
				object[] array3 = array as object[];
				if (array3 == null) {
					throw new ArgumentException("InvalidArrayType");
				}
				int count = dictionary.count;
				Entry[] entries = dictionary.entries;
				try {
					for (int i = 0; i < count; i++) {
						if (entries[i].hashCode >= 0) {
							array3[index++] = entries[i].key;
						}
					}
				} catch (ArrayTypeMismatchException) {
					throw new ArgumentException("InvalidArrayType");
				}
			}
		}

		public sealed class ValueCollection : ICollection<T>, IEnumerable<T>, IEnumerable, ICollection, IReadOnlyCollection<T> {
			public struct Enumerator : IEnumerator<T>, IDisposable, IEnumerator {
				private RTSDict<T> dictionary;
				private int index;
				private int version;
				private T currentValue;
				public T Current { get { return currentValue; } }
				object IEnumerator.Current {
					get {
						if (index == 0 || index == dictionary.count + 1) {
							throw new InvalidOperationException("EnumOpCantHappen");
						}
						return currentValue;
					}
				}
				internal Enumerator(RTSDict<T> dictionary) {
					this.dictionary = dictionary;
					version = dictionary.version;
					index = 0;
					currentValue = default(T);
				}
				public void Dispose() { }
				public bool MoveNext() {
					if (version != dictionary.version) {
						throw new InvalidOperationException("EnumFailedVersion");
					}
					while ((uint)index < (uint)dictionary.count) {
						if (dictionary.entries[index].hashCode >= 0) {
							currentValue = dictionary.entries[index].value;
							index++;
							return true;
						}
						index++;
					}
					index = dictionary.count + 1;
					currentValue = default(T);
					return false;
				}
				void IEnumerator.Reset() {
					if (version != dictionary.version) {
						throw new InvalidOperationException("EnumFailedVersion");
					}
					index = 0;
					currentValue = default(T);
				}
			}
			private RTSDict<T> dictionary;
			public int Count { get { return dictionary.Count; } }
			bool ICollection<T>.IsReadOnly { get { return true; } }
			bool ICollection.IsSynchronized { get { return false; } }
			object ICollection.SyncRoot { get { return ((ICollection)dictionary).SyncRoot; } }
			public ValueCollection(RTSDict<T> dictionary) {
				if (dictionary == null) {
					throw new ArgumentNullException("dictionary");
				}
				this.dictionary = dictionary;
			}
			public Enumerator GetEnumerator() {
				return new Enumerator(dictionary);
			}
			public void CopyTo(T[] array, int index) {
				if (array == null) {
					throw new ArgumentNullException("array");
				}
				if (index < 0 || index > array.Length) {
					throw new ArgumentOutOfRangeException("index", "NeedNonNegNum");
				}
				if (array.Length - index < dictionary.Count) {
					throw new ArgumentException("ArrayPlusOffTooSmall");
				}
				int count = dictionary.count;
				Entry[] entries = dictionary.entries;
				for (int i = 0; i < count; i++) {
					if (entries[i].hashCode >= 0) {
						array[index++] = entries[i].value;
					}
				}
			}
			void ICollection<T>.Add(T item) {
				throw new NotSupportedException("ValueCollectionSet");
			}
			bool ICollection<T>.Remove(T item) {
				throw new NotSupportedException("ValueCollectionSet");
			}
			void ICollection<T>.Clear() {
				throw new NotSupportedException("ValueCollectionSet");
			}
			bool ICollection<T>.Contains(T item) {
				return dictionary.ContainsValue(item);
			}
			IEnumerator<T> IEnumerable<T>.GetEnumerator() {
				return new Enumerator(dictionary);
			}
			IEnumerator IEnumerable.GetEnumerator() {
				return new Enumerator(dictionary);
			}
			void ICollection.CopyTo(Array array, int index) {
				if (array == null) {
					throw new ArgumentNullException("array");
				}
				if (array.Rank != 1) {
					throw new ArgumentException("RankMultiDimNotSupported");
				}
				if (array.GetLowerBound(0) != 0) {
					throw new ArgumentException("NonZeroLowerBound");
				}
				if (index < 0 || index > array.Length) {
					throw new ArgumentOutOfRangeException("index", "NeedNonNegNum");
				}
				if (array.Length - index < dictionary.Count) {
					throw new ArgumentException("ArrayPlusOffTooSmall");
				}
				T[] array2 = array as T[];
				if (array2 != null) {
					CopyTo(array2, index);
					return;
				}
				object[] array3 = array as object[];
				if (array3 == null) {
					throw new ArgumentException("InvalidArrayType");
				}
				int count = dictionary.count;
				Entry[] entries = dictionary.entries;
				try {
					for (int i = 0; i < count; i++) {
						if (entries[i].hashCode >= 0) {
							array3[index++] = entries[i].value;
						}
					}
				} catch (ArrayTypeMismatchException) {
					throw new ArgumentException("InvalidArrayType");
				}
			}
		}

		private OnModifyDelegate mOnItemModify;
		private OnListModifyDelegate mOnListItemModify;
		private OnDictModifyDelegate mOnDictItemModify;

		private List<string> mRemoved = new List<string>(16);

		private int[] buckets;

		private Entry[] entries;

		private int count;

		private int version;

		private int freeList;

		private int freeCount;

		private IEqualityComparer<string> comparer;

		private KeyCollection keys;

		private ValueCollection values;

		private object _syncRoot;

		private const string VersionName = "Version";

		private const string HashSizeName = "HashSize";

		private const string KeyValuePairsName = "KeyValuePairs";

		private const string ComparerName = "Comparer";

		public IEqualityComparer<string> Comparer { get { return comparer; } }

		public int Count { get { return count - freeCount; } }

		public KeyCollection Keys {
			get {
				if (keys == null) {
					keys = new KeyCollection(this);
				}
				return keys;
			}
		}

		ICollection<string> IDictionary<string, T>.Keys {
			get {
				if (keys == null) {
					keys = new KeyCollection(this);
				}
				return keys;
			}
		}

		IEnumerable<string> IReadOnlyDictionary<string, T>.Keys {
			get {
				if (keys == null) {
					keys = new KeyCollection(this);
				}
				return keys;
			}
		}

		public ValueCollection Values {
			get {
				if (values == null) {
					values = new ValueCollection(this);
				}
				return values;
			}
		}

		ICollection<T> IDictionary<string, T>.Values {
			get {
				if (values == null) {
					values = new ValueCollection(this);
				}
				return values;
			}
		}

		IEnumerable<T> IReadOnlyDictionary<string, T>.Values {
			get {
				if (values == null) {
					values = new ValueCollection(this);
				}
				return values;
			}
		}

		public T this[string key] {
			get {
				int num = FindEntry(key);
				if (num >= 0) {
					return entries[num].value;
				}
				throw new KeyNotFoundException();
			}
			set {
				Insert(key, value, add: false);
			}
		}

		bool ICollection<KeyValuePair<string, T>>.IsReadOnly { get { return false; } }

		bool ICollection.IsSynchronized { get { return false; } }

		object ICollection.SyncRoot {
			get {
				if (_syncRoot == null) {
					Interlocked.CompareExchange<object>(ref _syncRoot, new object(), (object)null);
				}
				return _syncRoot;
			}
		}

		bool IDictionary.IsFixedSize { get { return false; } }

		bool IDictionary.IsReadOnly { get { return false; } }

		ICollection IDictionary.Keys { get { return Keys; } }

		ICollection IDictionary.Values { get { return Values; } }

		object IDictionary.this[object key] {
			get {
				if (IsCompatibleKey(key)) {
					int num = FindEntry((string)key);
					if (num >= 0) {
						return entries[num].value;
					}
				}
				return null;
			}
			set {
				if (key == null) {
					throw new ArgumentNullException("key");
				}
				string key2 = (string)key;
				this[key2] = (T)value;
			}
		}

		public RTSDict() : this(0, (IEqualityComparer<string>)null) { }

		public RTSDict(int capacity) : this(capacity, (IEqualityComparer<string>)null) { }

		public RTSDict(IEqualityComparer<string> comparer) : this(0, comparer) { }

		public RTSDict(int capacity, IEqualityComparer<string> comparer) {
			if (capacity < 0) {
				throw new ArgumentOutOfRangeException("capacity");
			}
			if (capacity > 0) {
				Initialize(capacity);
			}
			this.comparer = (comparer ?? EqualityComparer<string>.Default);
			mOnItemModify = OnItemModify;
			mOnListItemModify = OnListItemModify;
			mOnDictItemModify = OnDictItemModify;
		}

		public RTSDict(IDictionary<string, T> dictionary) : this(dictionary, (IEqualityComparer<string>)null) { }

		public RTSDict(IDictionary<string, T> dictionary, IEqualityComparer<string> comparer) : this(dictionary?.Count ?? 0, comparer) {
			if (dictionary == null) {
				throw new ArgumentNullException("dictionary");
			}
			foreach (KeyValuePair<string, T> item in dictionary) {
				Add(item.Key, item.Value);
			}
			mOnItemModify = OnItemModify;
			mOnListItemModify = OnListItemModify;
			mOnDictItemModify = OnDictItemModify;
		}

		public void Add(string key, T value) {
			Insert(key, value, add: true);
		}

		void ICollection<KeyValuePair<string, T>>.Add(KeyValuePair<string, T> keyValuePair) {
			Add(keyValuePair.Key, keyValuePair.Value);
		}

		bool ICollection<KeyValuePair<string, T>>.Contains(KeyValuePair<string, T> keyValuePair) {
			int num = FindEntry(keyValuePair.Key);
			if (num >= 0 && EqualityComparer<T>.Default.Equals(entries[num].value, keyValuePair.Value)) {
				return true;
			}
			return false;
		}

		bool ICollection<KeyValuePair<string, T>>.Remove(KeyValuePair<string, T> keyValuePair) {
			int num = FindEntry(keyValuePair.Key);
			if (num >= 0 && EqualityComparer<T>.Default.Equals(entries[num].value, keyValuePair.Value)) {
				Remove(keyValuePair.Key);
				return true;
			}
			return false;
		}

		public void Clear() {
			if (count > 0) {
				for (int i = 0; i < buckets.Length; i++) {
					buckets[i] = -1;
				}
				Entry def = default(Entry);
				for (int i = 0; i < count; i++) {
					if (entries[i].hashCode >= 0) {
						TryClearHandlers(entries[i].value);
					}
					entries[i] = def;
				}
				freeList = -1;
				count = 0;
				freeCount = 0;
				version++;
				mRemoved.Clear();
				onModify?.Invoke(this, null);
			}
		}

		public bool ContainsKey(string key) {
			return FindEntry(key) >= 0;
		}

		public bool ContainsValue(T value) {
			if (value == null) {
				for (int i = 0; i < count; i++) {
					if (entries[i].hashCode >= 0 && entries[i].value == null) {
						return true;
					}
				}
			} else {
				EqualityComparer<T> @default = EqualityComparer<T>.Default;
				for (int j = 0; j < count; j++) {
					if (entries[j].hashCode >= 0 && @default.Equals(entries[j].value, value)) {
						return true;
					}
				}
			}
			return false;
		}

		private void CopyTo(KeyValuePair<string, T>[] array, int index) {
			if (array == null) {
				throw new ArgumentNullException("array");
			}
			if (index < 0 || index > array.Length) {
				throw new ArgumentOutOfRangeException("index", "NeedNonNegNum");
			}
			if (array.Length - index < Count) {
				throw new ArgumentException("ArrayPlusOffTooSmall");
			}
			int num = count;
			Entry[] array2 = entries;
			for (int i = 0; i < num; i++) {
				if (array2[i].hashCode >= 0) {
					array[index++] = new KeyValuePair<string, T>(array2[i].key, array2[i].value);
				}
			}
		}

		public Enumerator GetEnumerator() {
			return new Enumerator(this, 2);
		}

		IEnumerator<KeyValuePair<string, T>> IEnumerable<KeyValuePair<string, T>>.GetEnumerator() {
			return new Enumerator(this, 2);
		}

		void IDataLoggable.Reset() {
			mRemoved.Clear();
			for (int i = 0; i < count; i++) {
				Entry entry = entries[i];
				if (entry.hashCode < 0 || entry.s == 0) { continue; }
				entries[i].s = 0;
				IDataLoggable d = entry.value as IDataLoggable;
				if (d != null) { d.Reset(); }
			}
		}

		void IDataLoggable.CollectChangeLogs(string parent, IList<DataLog> logs) {
			for (int i = 0, imax = mRemoved.Count; i < imax; i++) {
				logs.Add(DataLog.Delete(parent, mRemoved[i]));
			}
			for (int i = 0; i < count; i++) {
				Entry entry = entries[i];
				if (entry.hashCode < 0 || entry.s == 0) { continue; }
				if (entry.s >= 2) {
					logs.Add(entry.s > 2 ? DataLog.Create(parent, entry.key, entry.value) : DataLog.Modify(parent, entry.key, entry.value));
					continue;
				}
				IDataLoggable d = entry.value as IDataLoggable;
				if (d != null) { d.CollectChangeLogs(string.IsNullOrEmpty(parent) ? entry.key : (parent + "." + entry.key), logs); }
			}
		}

		private int FindEntry(string key) {
			if (key == null) {
				throw new ArgumentNullException("key");
			}
			if (buckets != null) {
				int num = comparer.GetHashCode(key) & int.MaxValue;
				for (int num2 = buckets[num % buckets.Length]; num2 >= 0; num2 = entries[num2].next) {
					if (entries[num2].hashCode == num && comparer.Equals(entries[num2].key, key)) {
						return num2;
					}
				}
			}
			return -1;
		}

		private void Initialize(int capacity) {
			int prime = HashHelpers.GetPrime(capacity);
			buckets = new int[prime];
			for (int i = 0; i < buckets.Length; i++) {
				buckets[i] = -1;
			}
			entries = new Entry[prime];
			freeList = -1;
		}

		private void Insert(string key, T value, bool add) {
			if (key == null) {
				throw new ArgumentNullException("key");
			}
			if (buckets == null) {
				Initialize(0);
			}
			int num = comparer.GetHashCode(key) & int.MaxValue;
			int num2 = num % buckets.Length;
			int num3 = 0;
			for (int num4 = buckets[num2]; num4 >= 0; num4 = entries[num4].next) {
				if (entries[num4].hashCode == num && comparer.Equals(entries[num4].key, key)) {
					if (add) {
						throw new ArgumentException("AddingDuplicate");
					}
					T from = entries[num4].value;
					TryClearHandlers(from);
					entries[num4].value = value;
					entries[num4].s = 2;
					TryRegisterHandler(value);
					version++;
					mRemoved.Remove(key);
					onModify?.Invoke(this, key);
					return;
				}
				num3++;
			}
			int num5;
			if (freeCount > 0) {
				num5 = freeList;
				freeList = entries[num5].next;
				freeCount--;
			} else {
				if (count == entries.Length) {
					Resize();
					num2 = num % buckets.Length;
				}
				num5 = count;
				count++;
			}
			entries[num5].hashCode = num;
			entries[num5].next = buckets[num2];
			entries[num5].key = key;
			entries[num5].value = value;
			entries[num5].s = 3;
			buckets[num2] = num5;
			TryRegisterHandler(value);
			version++;
			if (num3 > 100) {
				comparer = new StringEqualityComparer();
				Resize(entries.Length, forceNewHashCodes: true);
			}
			mRemoved.Remove(key);
			onModify?.Invoke(this, key);
		}

		private void Resize() {
			Resize(HashHelpers.ExpandPrime(count), forceNewHashCodes: false);
		}

		private void Resize(int newSize, bool forceNewHashCodes) {
			int[] array = new int[newSize];
			for (int i = 0; i < array.Length; i++) {
				array[i] = -1;
			}
			Entry[] array2 = new Entry[newSize];
			Array.Copy(entries, 0, array2, 0, count);
			if (forceNewHashCodes) {
				for (int j = 0; j < count; j++) {
					if (array2[j].hashCode != -1) {
						array2[j].hashCode = (comparer.GetHashCode(array2[j].key) & int.MaxValue);
					}
				}
			}
			for (int k = 0; k < count; k++) {
				if (array2[k].hashCode >= 0) {
					int num = array2[k].hashCode % newSize;
					array2[k].next = array[num];
					array[num] = k;
				}
			}
			buckets = array;
			entries = array2;
		}

		public bool Remove(string key) {
			if (key == null) {
				throw new ArgumentNullException("key");
			}
			if (buckets != null) {
				int num = comparer.GetHashCode(key) & int.MaxValue;
				int num2 = num % buckets.Length;
				int num3 = -1;
				for (int num4 = buckets[num2]; num4 >= 0; num4 = entries[num4].next) {
					if (entries[num4].hashCode == num && comparer.Equals(entries[num4].key, key)) {
						if (num3 < 0) {
							buckets[num2] = entries[num4].next;
						} else {
							entries[num3].next = entries[num4].next;
						}
						T from = entries[num4].value;
						entries[num4].hashCode = -1;
						entries[num4].next = freeList;
						entries[num4].key = default(string);
						entries[num4].value = default(T);
						entries[num4].s = 0;
						freeList = num4;
						freeCount++;
						TryClearHandlers(from);
						version++;
						mRemoved.Add(key);
						onModify?.Invoke(this, key);
						return true;
					}
					num3 = num4;
				}
			}
			return false;
		}

		public bool TryGetValue(string key, out T value) {
			int num = FindEntry(key);
			if (num >= 0) {
				value = entries[num].value;
				return true;
			}
			value = default(T);
			return false;
		}

		internal T GetValueOrDefault(string key) {
			int num = FindEntry(key);
			if (num >= 0) {
				return entries[num].value;
			}
			return default(T);
		}

		void ICollection<KeyValuePair<string, T>>.CopyTo(KeyValuePair<string, T>[] array, int index) {
			CopyTo(array, index);
		}

		void ICollection.CopyTo(Array array, int index) {
			if (array == null) {
				throw new ArgumentNullException("array");
			}
			if (array.Rank != 1) {
				throw new ArgumentException("RankMultiDimNotSupported");
			}
			if (array.GetLowerBound(0) != 0) {
				throw new ArgumentException("NonZeroLowerBound");
			}
			if (index < 0 || index > array.Length) {
				throw new ArgumentOutOfRangeException("index", "NeedNonNegNum");
			}
			if (array.Length - index < Count) {
				throw new ArgumentException("ArrayPlusOffTooSmall");
			}
			KeyValuePair<string, T>[] array2 = array as KeyValuePair<string, T>[];
			if (array2 != null) {
				CopyTo(array2, index);
			} else if (array is DictionaryEntry[]) {
				DictionaryEntry[] array3 = array as DictionaryEntry[];
				Entry[] array4 = entries;
				for (int i = 0; i < count; i++) {
					if (array4[i].hashCode >= 0) {
						array3[index++] = new DictionaryEntry(array4[i].key, array4[i].value);
					}
				}
			} else {
				object[] array5 = array as object[];
				if (array5 == null) {
					throw new ArgumentException("InvalidArrayType");
				}
				try {
					int num = count;
					Entry[] array6 = entries;
					for (int j = 0; j < num; j++) {
						if (array6[j].hashCode >= 0) {
							array5[index++] = new KeyValuePair<string, T>(array6[j].key, array6[j].value);
						}
					}
				} catch (ArrayTypeMismatchException) {
					throw new ArgumentException("InvalidArrayType");
				}
			}
		}

		IEnumerator IEnumerable.GetEnumerator() {
			return new Enumerator(this, 2);
		}

		private static bool IsCompatibleKey(object key) {
			if (key == null) {
				throw new ArgumentNullException("key");
			}
			return key is string;
		}

		void IDictionary.Add(object key, object value) {
			if (key == null) {
				throw new ArgumentNullException("key");
			}
			string key2 = (string)key;
			Add(key2, (T)value);
		}

		bool IDictionary.Contains(object key) {
			if (IsCompatibleKey(key)) {
				return ContainsKey((string)key);
			}
			return false;
		}

		IDictionaryEnumerator IDictionary.GetEnumerator() {
			return new Enumerator(this, 1);
		}

		void IDictionary.Remove(object key) {
			if (IsCompatibleKey(key)) {
				Remove((string)key);
			}
		}

		private void TryRegisterHandler(object item) {
			DataNodeBase node = item as DataNodeBase;
			if (node != null) {
				node.onModify = mOnItemModify;
				return;
			}
			IRTSList list = item as IRTSList;
			if (list != null) {
				list.onModify = mOnListItemModify;
				return;
			}
			IRTSDict dict = item as IRTSDict;
			if (dict != null) {
				dict.onModify = mOnDictItemModify;
			}
		}

		private void TryClearHandlers(object item) {
			DataNodeBase node = item as DataNodeBase;
			if (node != null) { node.onModify = null; return; }
			IRTSList list = item as IRTSList;
			if (list != null) { list.onModify = null; return; }
			IRTSDict dict = item as IRTSDict;
			if (dict != null) { dict.onModify = null; return; }
		}

		private int IndexOf(T value) {
			EqualityComparer<T> @default = EqualityComparer<T>.Default;
			for (int i = 0; i < count; i++) {
				if (entries[i].hashCode >= 0 && @default.Equals(entries[i].value, value)) {
					return i;
				}
			}
			return -1;
		}

		private void OnItemModify(DataNodeBase data) {
			int index = IndexOf((T)(object)data);
			if (index < 0) { return; }
			Entry entry = entries[index];
			if (entry.s <= 0) {
				entry.s = 1;
				entries[index] = entry;
			}
			onModify?.Invoke(this, entry.key);
		}

		private void OnListItemModify(IRTSList list, int i) {
			int index = IndexOf((T)list);
			if (index < 0) { return; }
			Entry entry = entries[index];
			if (entry.s <= 0) {
				entry.s = (byte)(i < 0 ? 2 : 1);
				entries[index] = entry;
			}
			onModify?.Invoke(this, entry.key);
		}

		private void OnDictItemModify(IRTSDict dict, string k) {
			int index = IndexOf((T)dict);
			if (index < 0) { return; }
			Entry entry = entries[index];
			if (entry.s <= 0) {
				entry.s = (byte)(k == null ? 2 : 1);
				entries[index] = entry;
			}
			onModify?.Invoke(this, entry.key);
		}

		private static class HashHelpers {
			public static readonly int[] primes = new int[72] {
				3, 7, 11, 17, 23, 29, 37, 47, 59, 71, 89, 107, 131, 163, 197, 239, 293, 353, 431, 521, 631, 761, 919,
				1103, 1327, 1597, 1931, 2333, 2801, 3371, 4049, 4861, 5839, 7013, 8419, 10103, 12143, 14591, 17519,
				21023, 25229, 30293, 36353, 43627, 52361, 62851, 75431, 90523, 108631, 130363, 156437, 187751, 225307,
				270371, 324449, 389357, 467237, 560689, 672827, 807403, 968897, 1162687, 1395263, 1674319, 2009191,
				2411033, 2893249, 3471899, 4166287, 4999559, 5999471, 7199369 };
			[ReliabilityContract(Consistency.WillNotCorruptState, Cer.Success)]
			public static bool IsPrime(int candidate) {
				if ((candidate & 1) != 0) {
					int num = (int)Math.Sqrt(candidate);
					for (int i = 3; i <= num; i += 2) {
						if (candidate % i == 0) {
							return false;
						}
					}
					return true;
				}
				return candidate == 2;
			}
			[ReliabilityContract(Consistency.WillNotCorruptState, Cer.Success)]
			public static int GetPrime(int min) {
				if (min < 0) {
					throw new ArgumentException("HTCapacityOverflow");
				}
				for (int i = 0; i < primes.Length; i++) {
					int num = primes[i];
					if (num >= min) {
						return num;
					}
				}
				for (int j = min | 1; j < int.MaxValue; j += 2) {
					if (IsPrime(j) && (j - 1) % 101 != 0) {
						return j;
					}
				}
				return min;
			}
			public static int GetMinPrime() {
				return primes[0];
			}
			public static int ExpandPrime(int oldSize) {
				int num = 2 * oldSize;
				if ((uint)num > 2146435069u && 2146435069 > oldSize) {
					return 2146435069;
				}
				return GetPrime(num);
			}
		}

		private sealed class StringEqualityComparer : IEqualityComparer<string> {

			public bool Equals(string x, string y) {
				if (x != null) {
					if (y != null) {
						return x.Equals(y);
					}
					return false;
				}
				if (y != null) {
					return false;
				}
				return true;
			}

			public int GetHashCode(string obj) {
				return obj == null ? 0 : obj.GetHashCode();
			}

		}

	}

}
