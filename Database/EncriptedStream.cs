using System;
using System.IO;

namespace GreatClock.Common.RTS.DB {

	public sealed class EncriptedStream : IDisposable {

		private Stream mStream;

		public EncriptedStream(Stream stream) {
			if (stream == null) { throw new ArgumentNullException("stream"); }
			mStream = stream;
		}

		public long Position { get { return mStream.Position; } set { mStream.Position = value; } }

		public long Length { get { return mStream.Length; } }

		public int Read(byte[] buffer, int offset, int count) {
			int ret = mStream.Read(buffer, offset, count);
			// TODO decript
			return ret;
		}

		public void Write(byte[] buffer, int offset, int count) {
			// TODO encript
			mStream.Write(buffer, offset, count);
		}

		public void Flush() {
			mStream.Flush();
		}

		public void Dispose() {
			if (mStream != null) { mStream.Dispose(); }
			mStream = null;
		}

	}

}
