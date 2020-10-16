using GreatClock.Common.RTS.Collections;
using System;
using System.Collections.Generic;

namespace GreatClock.Common.RTS {

	public abstract class DataNodeBase : IDataLoggable {
		public OnModifyDelegate onModify { get; set; }
		void IDataLoggable.Reset() { OnReset(); }
		void IDataLoggable.CollectChangeLogs(string parent, IList<DataLog> logs) { OnCollectChangeLogs(parent, logs); }
		protected abstract void OnReset();
		protected abstract void OnCollectChangeLogs(string parent, IList<DataLog> logs);
	}

	public struct DataLog {
		public string path;
		public char op; // * - +
		public object value;
		public static DataLog Modify(string parent, string name, object value) {
			return new DataLog() { path = string.IsNullOrEmpty(parent) ? name : (parent + "." + name), op = '*', value = value };
		}
		public static DataLog Delete(string parent, string name) {
			return new DataLog() { path = string.IsNullOrEmpty(parent) ? name : (parent + "." + name), op = '-', value = null };
		}
		public static DataLog Create(string parent, string name, object value) {
			return new DataLog() { path = string.IsNullOrEmpty(parent) ? name : (parent + "." + name), op = '+', value = value };
		}
	}

	public interface IDataLoggable {
		void CollectChangeLogs(string parent, IList<DataLog> logs);
		void Reset();
	}

	public delegate void OnModifyDelegate(DataNodeBase data);

	public delegate void OnListModifyDelegate(IRTSList list, int index);

	public delegate void OnDictModifyDelegate(IRTSDict dict, string key);

	[AttributeUsage(AttributeTargets.Field, AllowMultiple = false)]
	public class RTSSerializeFieldAttribute : Attribute {
		public readonly string Name;
		public RTSSerializeFieldAttribute(string name) {
			Name = name;
		}
	}

}