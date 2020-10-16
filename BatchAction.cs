using System;
using System.Threading;

namespace GreatClock.Common.RTS {

	public sealed class BatchAction<T> where T : class {

		private int mDelay;
		private Action<T> mAction;

		private Item mFirst;
		private Item mLast;

		private ManualResetEvent mBlock = new ManualResetEvent(false);
		private bool mThreadRunning = false;

		public BatchAction(int delay, Action<T> action) {
			mDelay = delay;
			mAction = action;
			mFirst = new Item(null);
			mLast = new Item(null);
			mLast.InsertAfter(mFirst);
		}

		public bool Enqueue(T item) {
			if (item == null) { return false; }
			long time = DateTime.UtcNow.Ticks + mDelay * 10000L;
			lock (mFirst) {
				Item current = mFirst.Next;
				while (current != mLast) {
					if (current.data == item) {
						current.time = time;
						current.RemoveSelf().InsertBefore(mLast);
						return true;
					}
					current = current.Next;
				}
				current = new Item(item);
				current.time = time;
				current.InsertBefore(mLast);
				mBlock.Set();
			}
			if (!mThreadRunning) {
				mThreadRunning = true;
				new Thread(new ThreadStart(Run)).Start();
			}
			return true;
		}

		public bool Remove(T item) {
			lock (mFirst) {
				Item current = mFirst.Next;
				while (current != mLast) {
					if (current.data == item) {
						current.RemoveSelf();
						return true;
					}
				}
			}
			return false;
		}

		public void Stop() {
			mThreadRunning = false;
		}

		private class Item {
			public readonly T data;
			public long time;
			public Item Prev { get; private set; }
			public Item Next { get; private set; }
			public Item(T data) { this.data = data; }
			public Item RemoveSelf() {
				Item prev = Prev;
				Item next = Next;
				Prev = null;
				Next = null;
				if (prev != null) { prev.Next = next; }
				if (next != null) { next.Prev = prev; }
				return this;
			}
			public Item InsertBefore(Item other) {
				if (other == null) { return this; }
				Item prev = other.Prev;
				if (prev != null) { prev.Next = this; }
				other.Prev = this;
				Prev = prev;
				Next = other;
				return this;
			}
			public Item InsertAfter(Item other) {
				if (other == null) { return this; }
				Item next = other.Next;
				if (next != null) { next.Prev = this; }
				other.Next = this;
				Prev = other;
				Next = next;
				return this;
			}
		}

		private void Run() {
			while (mThreadRunning) {
				mBlock.WaitOne();
				long delay = 0L;
				Item item = null;
				lock (mFirst) {
					Item current = mFirst.Next;
					if (current == mLast) {
						mBlock.Reset();
					} else {
						delay = current.time - DateTime.UtcNow.Ticks;
						if (delay <= 0L) {
							item = current.RemoveSelf();
						}
					}
				}
				if (item != null) { mAction(item.data); }
				if (delay > 0L) { Thread.Sleep((int)(delay / 10000L)); }
			}
		}

	}

}
