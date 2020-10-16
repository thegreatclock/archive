using System;
using System.Collections.Generic;
using System.Reflection;

namespace GreatClock.Common.RTS.Utilities {

	public class NodeTypeFieldData {

		public struct FieldData {
			public string name;
			public FieldInfo field;
			public PropertyInfo property;
		}

		public static NodeTypeFieldData GetNodeTypeFieldData(Type type) {
			if (!type.IsSubclassOf(type_data_node_base)) { return null; }
			NodeTypeFieldData data;
			lock (node_types) {
				if (!node_types.TryGetValue(type, out data)) {
					data = new NodeTypeFieldData(type);
					node_types.Add(type, data);
				}
			}
			return data;
		}

		private static Dictionary<Type, NodeTypeFieldData> node_types = new Dictionary<Type, NodeTypeFieldData>(64);

		private static Type type_data_node_base = typeof(DataNodeBase);

		private Dictionary<string, FieldData> mFields = new Dictionary<string, FieldData>(16);
		private FieldData[] mFieldsArray;

		public NodeTypeFieldData(Type type) {
			if (!type.IsSubclassOf(type_data_node_base)) { return; }
			Stack<Type> types = new Stack<Type>();
			Type t = type;
			while (t.IsSubclassOf(type_data_node_base)) {
				types.Push(t);
				t = t.BaseType;
			}
			types.Push(type_data_node_base);
			List<FieldData> list = new List<FieldData>(16);
			while (types.Count > 0) {
				t = types.Pop();
				FieldInfo[] fields = t.GetFields(BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic);
				for (int i = 0; i < fields.Length; i++) {
					FieldInfo field = fields[i];
					RTSSerializeFieldAttribute attr = field.GetCustomAttribute<RTSSerializeFieldAttribute>();
					if (attr == null) { continue; }
					if (string.IsNullOrEmpty(attr.Name)) { continue; }
					if (mFields.ContainsKey(attr.Name)) { continue; }
					PropertyInfo prop = t.GetProperty(attr.Name, BindingFlags.Instance | BindingFlags.Public);
					FieldData fd = new FieldData() { name = attr.Name, field = field, property = prop };
					mFields.Add(attr.Name, fd);
					list.Add(fd);
				}
			}
			mFieldsArray = list.ToArray();
		}

		public FieldData GetField(string name) {
			FieldData field;
			return mFields.TryGetValue(name, out field) ? field : new FieldData();
		}

		public FieldData[] GetFields() {
			return mFieldsArray;
		}

	}

}
