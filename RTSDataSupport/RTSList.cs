using System;
using System.Collections;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Globalization;
using System.Threading;

namespace GreatClock.Common.RTS.Collections {

	public interface IRTSList : ICollection, IEnumerable, IDataLoggable {
		OnListModifyDelegate onModify { get; set; }
		double GetPriority(int i);
		int RawAdd(double p, object v);
		void Add(object v);
	}

	public static class RTSListUtil {
		public static string ToKey(double _) {
			return "_" + _.ToString("R", CultureInfo.InvariantCulture).Replace('.', '_');
		}
		public static double FromKey(string key) {
			string str = key.Substring(1, key.Length - 1);
			str.Replace('_', '.');
			double _;
			return double.TryParse(str, NumberStyles.Float, CultureInfo.InvariantCulture, out _) ? _ : double.NaN;
		}
	}

	public class RTSList<T> : IList<T>, ICollection<T>, IEnumerable<T>, IEnumerable, IList, ICollection, IReadOnlyList<T>, IReadOnlyCollection<T>, IRTSList {

		public struct Enumerator : IEnumerator<T>, IDisposable, IEnumerator {

			private RTSList<T> list;

			private int index;

			private int version;

			private T current;

			public T Current {
				get {
					return current;
				}
			}

			object IEnumerator.Current {
				get {
					if (index == 0 || index == list._size + 1) {
						throw new InvalidOperationException();
					}
					return Current;
				}
			}

			internal Enumerator(RTSList<T> list) {
				this.list = list;
				index = 0;
				version = list._version;
				current = default(T);
			}

			public void Dispose() { }

			public bool MoveNext() {
				RTSList<T> list = this.list;
				if (version == list._version && (uint)index < (uint)list._size) {
					current = list._items[index].v;
					index++;
					return true;
				}
				return MoveNextRare();
			}

			private bool MoveNextRare() {
				if (version != list._version) {
					throw new InvalidOperationException();
				}
				index = list._size + 1;
				current = default(T);
				return false;
			}

			void IEnumerator.Reset() {
				if (version != list._version) {
					throw new InvalidOperationException();
				}
				index = 0;
				current = default(T);
			}
		}

		private struct Node {
			public double p;
			public T v;
			public byte s;
			public Node(double p, T v, byte s) { this.p = p; this.v = v; this.s = s; }
		}

		private class NodeComparer : IComparer<Node> {
			private IComparer<T> mComparer;
			public NodeComparer(IComparer<T> comparer) {
				mComparer = comparer;
			}
			public int Compare(Node x, Node y) {
				return mComparer.Compare(x.v, y.v);
			}
		}

		private Node[] _items;

		private int _size;

		private int _version;

		[NonSerialized]
		private object _syncRoot;

		private static readonly Node[] _emptyArray = new Node[0];

		public OnListModifyDelegate onModify { get; set; }

		private OnModifyDelegate mOnItemModify;
		private OnListModifyDelegate mOnListItemModify;
		private OnDictModifyDelegate mOnDictItemModify;

		public int Capacity {
			get {
				return _items.Length;
			}
			set {
				if (value < _size) {
					throw new ArgumentOutOfRangeException("Capacity");
				}
				if (value == _items.Length) {
					return;
				}
				if (value > 0) {
					Node[] array = new Node[value];
					if (_size > 0) {
						Array.Copy(_items, 0, array, 0, _size);
					}
					_items = array;
				} else {
					_items = _emptyArray;
				}
			}
		}

		public int Count { get { return _size; } }

		bool IList.IsFixedSize { get { return false; } }

		bool ICollection<T>.IsReadOnly { get { return false; } }

		bool IList.IsReadOnly { get { return false; } }

		bool ICollection.IsSynchronized { get { return false; } }

		object ICollection.SyncRoot {
			get {
				if (_syncRoot == null) {
					Interlocked.CompareExchange<object>(ref _syncRoot, new object(), (object)null);
				}
				return _syncRoot;
			}
		}

		public T this[int index] {
			get {
				if ((uint)index >= (uint)_size) {
					throw new ArgumentOutOfRangeException();
				}
				return _items[index].v;
			}
			set {
				if ((uint)index >= (uint)_size) {
					throw new ArgumentOutOfRangeException();
				}
				T from = _items[index].v;
				TryClearHandlers(from);
				Node item = _items[index];
				item.v = value;
				_items[index] = item;
				TryRegisterHandler(item);
				_version++;
				onModify?.Invoke(this, index);
			}
		}

		object IList.this[int index] { get { return this[index]; } set { this[index] = (T)value; } }

		double IRTSList.GetPriority(int index) {
			if (index < 0 || index >= _size) { return double.NaN; }
			return _items[index].p;
		}

		int IRTSList.RawAdd(double p, object v) {
			if (_size == _items.Length) {
				EnsureCapacity(_size + 1);
			}
			T item = (T)v;
			TryRegisterHandler(item);
			int l = 0;
			int r = _size;
			while (l < r) {
				int i = (l + r) >> 1;
				if (p > _items[i].p) { l = i + 1; } else { r = i; }
			}
			if (r < _size) {
				Array.Copy(_items, r, _items, r + 1, _size - r);
			}
			_items[r] = new Node(p, item, 0);
			_size++;
			return r;
		}

		public RTSList() {
			_items = _emptyArray;
			mOnItemModify = OnItemModify;
			mOnListItemModify = OnListItemModify;
			mOnDictItemModify = OnDictItemModify;
		}

		public RTSList(int capacity) {
			if (capacity < 0) {
				throw new ArgumentOutOfRangeException("capacity");
			}
			if (capacity == 0) {
				_items = _emptyArray;
			} else {
				_items = new Node[capacity];
			}
			mOnItemModify = OnItemModify;
			mOnListItemModify = OnListItemModify;
			mOnDictItemModify = OnDictItemModify;
		}

		public RTSList(IEnumerable<T> collection) {
			if (collection == null) {
				throw new ArgumentNullException("collection");
			}
			ICollection<T> collection2 = collection as ICollection<T>;
			if (collection2 != null) {
				int count = collection2.Count;
				if (count == 0) {
					_items = _emptyArray;
					return;
				}
				_items = new Node[count];
				int i = 0;
				foreach (T item in collection2) {
					_items[i++] = new Node(i, item, 0);
				}
				_size = count;
			} else {
				_size = 0;
				_items = _emptyArray;
				foreach (T item in collection) {
					Add(item);
				}
			}
			mOnItemModify = OnItemModify;
			mOnListItemModify = OnListItemModify;
			mOnDictItemModify = OnDictItemModify;
		}

		private static bool IsCompatibleObject(object value) {
			if (!(value is T)) {
				if (value == null) {
					return default(T) == null;
				}
				return false;
			}
			return true;
		}

		public void Add(T item) {
			if (_size == _items.Length) {
				EnsureCapacity(_size + 1);
			}
			double p = _size > 0 ? _items[_size - 1].p + 1.0 : 0.0;
			TryRegisterHandler(item);
			Node n = new Node(p, item, 3);
			_items[_size] = n;
			_size++;
			_version++;
			string pp = RTSListUtil.ToKey(n.p);
			mRemoved.Remove(pp);
			onModify?.Invoke(this, _size - 1);
		}

		int IList.Add(object item) {
			Add((T)item);
			return Count - 1;
		}

		void IRTSList.Add(object item) {
			Add((T)item);
		}

		private List<string> mRemoved = new List<string>(16);

		void IDataLoggable.Reset() {
			mRemoved.Clear();
			for (int i = 0; i < _size; i++) {
				Node n = _items[i];
				if (n.s == 0) { continue; }
				n.s = 0;
				_items[i] = n;
				IDataLoggable d = n.v as IDataLoggable;
				if (d != null) { d.Reset(); }
			}
		}

		void IDataLoggable.CollectChangeLogs(string parent, IList<DataLog> logs) {
			for (int i = 0, imax = mRemoved.Count; i < imax; i++) {
				logs.Add(DataLog.Delete(parent, mRemoved[i]));
			}
			for (int i = 0; i < _size; i++) {
				Node n = _items[i];
				if (n.s == 0) { continue; }
				string p = RTSListUtil.ToKey(n.p);
				if (n.s >= 2) {
					logs.Add(n.s > 2 ? DataLog.Create(parent, p, n.v) : DataLog.Modify(parent, p, n.v));
					continue;
				}
				IDataLoggable d = n.v as IDataLoggable;
				if (d != null) { d.CollectChangeLogs(string.IsNullOrEmpty(parent) ? p : (parent + "." + p), logs); }
			}
		}

		public void AddRange(IEnumerable<T> collection) {
			InsertRange(_size, collection);
		}

		public ReadOnlyCollection<T> AsReadOnly() {
			return new ReadOnlyCollection<T>(this);
		}

		public void Clear() {
			Node def = default(Node);
			for (int i = 0; i < _size; i++) {
				TryClearHandlers(_items[i].v);
				_items[i] = def;
			}
			_size = 0;
			_version++;
			mRemoved.Clear();
			onModify?.Invoke(this, -1);
		}

		public bool Contains(T item) {
			if (item == null) {
				for (int i = 0; i < _size; i++) {
					if (_items[i].v == null) {
						return true;
					}
				}
				return false;
			}
			EqualityComparer<T> @default = EqualityComparer<T>.Default;
			for (int j = 0; j < _size; j++) {
				if (@default.Equals(_items[j].v, item)) {
					return true;
				}
			}
			return false;
		}

		bool IList.Contains(object item) {
			if (IsCompatibleObject(item)) {
				return Contains((T)item);
			}
			return false;
		}

		public RTSList<TOutput> ConvertAll<TOutput>(Converter<T, TOutput> converter) {
			if (converter == null) {
				throw new ArgumentNullException("converter");
			}
			RTSList<TOutput> list = new RTSList<TOutput>(_size);
			for (int i = 0; i < _size; i++) {
				Node item = _items[i];
				list._items[i] = new RTSList<TOutput>.Node(item.p, converter(item.v), 0);
			}
			list._size = _size;
			return list;
		}

		public void CopyTo(T[] array) {
			CopyTo(array, 0);
		}

		void ICollection.CopyTo(Array array, int arrayIndex) {
			if (array != null && array.Rank != 1) {
				throw new ArgumentException("Arg RankMultiDimNotSupported");
			}
			try {
				Array.Copy(_items, 0, array, arrayIndex, _size);
			} catch (ArrayTypeMismatchException) {
				throw new ArgumentException("Argument_InvalidArrayType");
			}
		}

		public void CopyTo(int index, T[] array, int arrayIndex, int count) {
			if (_size - index < count) {
				throw new ArgumentException("Argument_InvalidOffLen");
			}
			Array.Copy(_items, index, array, arrayIndex, count);
		}

		public void CopyTo(T[] array, int arrayIndex) {
			Array.Copy(_items, 0, array, arrayIndex, _size);
		}

		private void EnsureCapacity(int min) {
			if (_items.Length < min) {
				int num = (_items.Length == 0) ? 4 : (_items.Length * 2);
				if ((uint)num > 2146435071u) {
					num = 2146435071;
				}
				if (num < min) {
					num = min;
				}
				Capacity = num;
			}
		}

		public bool Exists(Predicate<T> match) {
			return FindIndex(match) != -1;
		}

		public T Find(Predicate<T> match) {
			if (match == null) {
				throw new ArgumentNullException("match");
			}
			for (int i = 0; i < _size; i++) {
				if (match(_items[i].v)) {
					return _items[i].v;
				}
			}
			return default(T);
		}

		public RTSList<T> FindAll(Predicate<T> match) {
			if (match == null) {
				throw new ArgumentNullException("match");
			}
			RTSList<T> list = new RTSList<T>();
			for (int i = 0; i < _size; i++) {
				if (match(_items[i].v)) {
					list.Add(_items[i].v);
				}
			}
			return list;
		}

		public int FindIndex(Predicate<T> match) {
			return FindIndex(0, _size, match);
		}

		public int FindIndex(int startIndex, Predicate<T> match) {
			return FindIndex(startIndex, _size - startIndex, match);
		}

		public int FindIndex(int startIndex, int count, Predicate<T> match) {
			if ((uint)startIndex > (uint)_size) {
				throw new ArgumentOutOfRangeException("startIndex");
			}
			if (count < 0 || startIndex > _size - count) {
				throw new ArgumentOutOfRangeException("count");
			}
			if (match == null) {
				throw new ArgumentNullException("match");
			}
			int num = startIndex + count;
			for (int i = startIndex; i < num; i++) {
				if (match(_items[i].v)) {
					return i;
				}
			}
			return -1;
		}

		public T FindLast(Predicate<T> match) {
			if (match == null) {
				throw new ArgumentNullException("match");
			}
			for (int num = _size - 1; num >= 0; num--) {
				if (match(_items[num].v)) {
					return _items[num].v;
				}
			}
			return default(T);
		}

		public int FindLastIndex(Predicate<T> match) {
			return FindLastIndex(_size - 1, _size, match);
		}

		public int FindLastIndex(int startIndex, Predicate<T> match) {
			return FindLastIndex(startIndex, startIndex + 1, match);
		}

		public int FindLastIndex(int startIndex, int count, Predicate<T> match) {
			if (match == null) {
				throw new ArgumentNullException("match");
			}
			if (_size == 0) {
				if (startIndex != -1) {
					throw new ArgumentOutOfRangeException("startIndex");
				}
			} else if ((uint)startIndex >= (uint)_size) {
				throw new ArgumentOutOfRangeException("startIndex");
			}
			if (count < 0 || startIndex - count + 1 < 0) {
				throw new ArgumentOutOfRangeException("count");
			}
			int num = startIndex - count;
			for (int num2 = startIndex; num2 > num; num2--) {
				if (match(_items[num2].v)) {
					return num2;
				}
			}
			return -1;
		}

		public void ForEach(Action<T> action) {
			if (action == null) {
				throw new ArgumentNullException("match");
			}
			int version = _version;
			for (int i = 0; i < _size; i++) {
				if (version != _version) {
					break;
				}
				action(_items[i].v);
			}
			if (version != _version) {
				throw new InvalidOperationException();
			}
		}

		public Enumerator GetEnumerator() {
			return new Enumerator(this);
		}

		IEnumerator<T> IEnumerable<T>.GetEnumerator() {
			return new Enumerator(this);
		}

		IEnumerator IEnumerable.GetEnumerator() {
			return new Enumerator(this);
		}

		public RTSList<T> GetRange(int index, int count) {
			if (index < 0) {
				throw new ArgumentOutOfRangeException("index");
			}
			if (count < 0) {
				throw new ArgumentOutOfRangeException("count");
			}
			if (_size - index < count) {
				throw new ArgumentException("Argument_InvalidOffLen");
			}
			RTSList<T> list = new RTSList<T>(count);
			Array.Copy(_items, index, list._items, 0, count);
			list._size = count;
			return list;
		}

		public int IndexOf(T item) {
			return IndexOf(item, 0, _size);
		}

		int IList.IndexOf(object item) {
			if (IsCompatibleObject(item)) {
				return IndexOf((T)item);
			}
			return -1;
		}

		public int IndexOf(T item, int index) {
			return IndexOf(item, index, _size - index);
		}

		public int IndexOf(T item, int index, int count) {
			if (index > _size) {
				throw new ArgumentOutOfRangeException("index");
			}
			if (count < 0 || index > _size - count) {
				throw new ArgumentOutOfRangeException("count");
			}
			if (item == null) {
				for (int i = 0; i < count; i++) {
					if (_items[i + index].v == null) { return i + index; }
				}
				return -1;
			}
			EqualityComparer<T> @default = EqualityComparer<T>.Default;
			for (int i = 0; i < count; i++) {
				if (@default.Equals(_items[i + index].v, item)) { return i + index; }
			}
			return -1;
		}

		public void Insert(int index, T item) {
			if ((uint)index > (uint)_size) {
				throw new ArgumentOutOfRangeException("index");
			}
			if (_size == _items.Length) {
				EnsureCapacity(_size + 1);
			}
			if (index < _size) {
				Array.Copy(_items, index, _items, index + 1, _size - index);
			}
			double p = 0.0;
			if (_size > 0) {
				if (index == 0) { p = _items[1].p - 1.0; } else if (index == _size) { p = _items[_size].p + 1.0; } else { p = (_items[index - 1].p + _items[index + 1].p) * 0.5; }
			}
			Node n = new Node(p, item, 3);
			_items[index] = n;
			_size++;
			TryRegisterHandler(item);
			_version++;
			string pp = RTSListUtil.ToKey(n.p);
			mRemoved.Remove(pp);
			onModify?.Invoke(this, index);
		}

		void IList.Insert(int index, object item) {
			Insert(index, (T)item);
		}

		public void InsertRange(int index, IEnumerable<T> collection) {
			if (collection == null) {
				throw new ArgumentNullException("collection");
			}
			if ((uint)index > (uint)_size) {
				throw new ArgumentOutOfRangeException("index");
			}
			ICollection<T> collection2 = typeof(T).IsSubclassOf(typeof(DataNodeBase)) ? null : collection as ICollection<T>;
			if (collection2 != null) {
				int count = collection2.Count;
				if (count > 0) {
					EnsureCapacity(_size + count);
					if (index < _size) {
						Array.Copy(_items, index, _items, index + count, _size - index);
					}
					if (this == collection2) {
						Array.Copy(_items, 0, _items, index, index);
						Array.Copy(_items, index + count, _items, index * 2, _size - index);
					} else {
						T[] array = new T[count];
						collection2.CopyTo(array, 0);
						array.CopyTo(_items, index);
					}
					_size += count;
				}
				_version++;
				onModify?.Invoke(this, -1);
			} else {
				using (IEnumerator<T> enumerator = collection.GetEnumerator()) {
					while (enumerator.MoveNext()) {
						Insert(index++, enumerator.Current);
					}
				}
			}
		}

		public int LastIndexOf(T item) {
			if (_size == 0) {
				return -1;
			}
			return LastIndexOf(item, _size - 1, _size);
		}

		public int LastIndexOf(T item, int index) {
			if (index >= _size) {
				throw new ArgumentOutOfRangeException("index");
			}
			return LastIndexOf(item, index, index + 1);
		}

		public int LastIndexOf(T item, int index, int count) {
			if (Count != 0 && index < 0) {
				throw new ArgumentOutOfRangeException("index");
			}
			if (Count != 0 && count < 0) {
				throw new ArgumentOutOfRangeException("count");
			}
			if (_size == 0) {
				return -1;
			}
			if (index >= _size) {
				throw new ArgumentOutOfRangeException("index");
			}
			if (count > index + 1) {
				throw new ArgumentOutOfRangeException("count");
			}
			return Array.LastIndexOf(_items, item, index, count);
		}

		public bool Remove(T item) {
			int num = IndexOf(item);
			if (num >= 0) {
				RemoveAt(num);
				return true;
			}
			return false;
		}

		void IList.Remove(object item) {
			if (IsCompatibleObject(item)) {
				Remove((T)item);
			}
		}

		public int RemoveAll(Predicate<T> match) {
			if (match == null) {
				throw new ArgumentNullException("match");
			}
			int i;
			for (i = 0; i < _size && !match(_items[i].v); i++) {
			}
			if (i >= _size) {
				return 0;
			}
			int j = i + 1;
			while (j < _size) {
				for (; j < _size && match(_items[j].v); j++) {
				}
				if (j < _size) {
					TryClearHandlers(_items[i].v);
					_items[i++] = _items[j++];
				}
			}
			Array.Clear(_items, i, _size - i);
			int result = _size - i;
			_size = i;
			_version++;
			onModify?.Invoke(this, -1);
			return result;
		}

		public void RemoveAt(int index) {
			if ((uint)index >= (uint)_size) {
				throw new ArgumentOutOfRangeException("index");
			}
			Node from = _items[index];
			TryClearHandlers(from.v);
			_size--;
			if (index < _size) {
				Array.Copy(_items, index + 1, _items, index, _size - index);
			}
			Node item = _items[_size];
			item.v = default(T);
			_items[_size] = item;
			_version++;
			string p = RTSListUtil.ToKey(from.p);
			mRemoved.Add(p);
			onModify?.Invoke(this, index);
		}

		public void RemoveRange(int index, int count) {
			if (index < 0) {
				throw new ArgumentOutOfRangeException("index");
			}
			if (count < 0) {
				throw new ArgumentOutOfRangeException("count");
			}
			if (_size - index < count) {
				throw new ArgumentException("Argument_InvalidOffLen");
			}
			if (count > 0) {
				for (int i = index; i < index + count; i++) {
					DataNodeBase node = _items[i].v as DataNodeBase;
					if (node != null) { node.onModify = null; }
				}
				int size = _size;
				_size -= count;
				if (index < _size) {
					Array.Copy(_items, index + count, _items, index, _size - index);
				}
				Array.Clear(_items, _size, count);
				_version++;
				onModify?.Invoke(this, -1);
			}
		}

		public void Reverse() {
			Reverse(0, Count);
		}

		public void Reverse(int index, int count) {
			if (index < 0) {
				throw new ArgumentOutOfRangeException("index");
			}
			if (count < 0) {
				throw new ArgumentOutOfRangeException("count");
			}
			if (_size - index < count) {
				throw new ArgumentException("Argument_InvalidOffLen");
			}
			for (int il = index, ir = index + count - 1; il < ir; il++, ir++) {
				Node nl = _items[il];
				Node nr = _items[ir];
				T v = nl.v;
				nl.v = nr.v;
				nr.v = v;
				_items[il] = nl;
				_items[ir] = nr;
			}
			_version++;
			onModify?.Invoke(this, -1);
		}

		public void Sort() {
			Sort(0, Count, null);
		}

		public void Sort(IComparer<T> comparer) {
			Sort(0, Count, comparer);
		}

		public void Sort(int index, int count, IComparer<T> comparer) {
			if (index < 0) {
				throw new ArgumentOutOfRangeException("index");
			}
			if (count < 0) {
				throw new ArgumentOutOfRangeException("count");
			}
			if (_size - index < count) {
				throw new ArgumentException("Argument_InvalidOffLen");
			}
			Array.Sort(_items, index, count, new NodeComparer(comparer));
			UpdatePriority(index, count);
			_version++;
			onModify?.Invoke(this, -1);
		}

		public void Sort(Comparison<T> comparison) {
			if (comparison == null) {
				throw new ArgumentNullException("comparison");
			}
			if (_size > 0) {
				IComparer<T> comparer = new FunctorComparer(comparison);
				Array.Sort(_items, 0, _size, new NodeComparer(comparer));
				UpdatePriority(0, _size);
				_version++;
				onModify?.Invoke(this, -1);
			}
		}

		public void UpdatePriority(int index, int count) {
			if (count <= 0) { return; }
			int ir = index + count;
			for (int i = index; i < index + count; i++) {
				int il = i - 1;
				Node n = _items[i];
				if (il >= 0 && ir >= _size) { if (n.p <= _items[il].p) { n.p = _items[il].p + 1.0; } }
				if (il < 0 && ir < _size) { if (n.p >= _items[ir].p) { n.p = _items[ir].p - 1.0; } }
				if (il >= 0 && ir < _size) { if (n.p <= _items[il].p || n.p >= _items[ir].p) { n.p = (_items[ir].p - _items[il].p) / (count - il) + _items[il].p; } }
			}
		}

		public T[] ToArray() {
			T[] array = new T[_size];
			Array.Copy(_items, 0, array, 0, _size);
			return array;
		}

		public void TrimExcess() {
			int num = (int)((double)_items.Length * 0.9);
			if (_size < num) {
				Capacity = _size;
			}
		}

		public bool TrueForAll(Predicate<T> match) {
			if (match == null) {
				throw new ArgumentNullException("match");
			}
			for (int i = 0; i < _size; i++) {
				if (!match(_items[i].v)) {
					return false;
				}
			}
			return true;
		}

		public override string ToString() {
			string[] strings = new string[_size];
			for (int i = 0; i < _size; i++) {
				T value = _items[i].v;
				strings[i] = value == null ? "null" : value.ToString();
			}
			return string.Concat("[", string.Join(", ", strings), "]");
		}

		private void OnItemModify(DataNodeBase data) {
			int index = IndexOf((T)(object)data);
			if (index < 0) { return; }
			Node n = _items[index];
			if (n.s <= 0) {
				n.s = 1;
				_items[index] = n;
			}
			onModify?.Invoke(this, index);
		}

		private void OnListItemModify(IRTSList list, int i) {
			int index = IndexOf((T)list);
			if (index < 0) { return; }
			Node n = _items[index];
			if (n.s <= 0) {
				n.s = (byte)(i < 0 ? 2 : 1);
				_items[index] = n;
			}
			_items[index] = n;
			onModify?.Invoke(this, index);
		}

		private void OnDictItemModify(IRTSDict dict, string k) {
			int index = IndexOf((T)dict);
			if (index < 0) { return; }
			Node n = _items[index];
			if (n.s <= 0) {
				n.s = (byte)(k == null ? 2 : 1);
				_items[index] = n;
			}
			_items[index] = n;
			onModify?.Invoke(this, index);
		}

		sealed class FunctorComparer : IComparer<T> {
			private Comparison<T> comparison;

			public FunctorComparer(Comparison<T> comparison) {
				this.comparison = comparison;
			}

			public int Compare(T x, T y) {
				return comparison(x, y);
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

	}

}
