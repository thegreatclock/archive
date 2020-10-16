using System;
using System.Globalization;
using System.IO;
using System.Reflection;
using System.Text;
using UnityEditor;
using UnityEngine;


namespace GreatClock.Common.RTS.DB {

	public class KVDB2Json {

		[MenuItem("GreatClock/RTS/KVDB To Json")]
		static void KVDBToJson() {
			string dbPath = EditorUtility.OpenFilePanel("Select DB file", Application.persistentDataPath, "");
			if (string.IsNullOrEmpty(dbPath)) { return; }
			KVDB db = new KVDB(-1, dbPath);
			try {
				FieldInfo fRoot = typeof(KVDB).GetField("mRoot", BindingFlags.Instance | BindingFlags.NonPublic);
				IDataReader dr = fRoot.GetValue(db) as IDataReader;
				StringBuilder json = new StringBuilder();
				ToJson(dr, json, "");
				string jsonDir = Application.dataPath;
				jsonDir = jsonDir.Substring(0, jsonDir.Length - 7);
				string path = EditorUtility.SaveFilePanel("Save json File", jsonDir, Path.GetFileNameWithoutExtension(dbPath), "json");
				if (string.IsNullOrEmpty(path)) {
					Debug.Log(json.ToString());
				} else {
					File.WriteAllText(path, json.ToString(), Encoding.UTF8);
				}
			} catch (Exception e) {
				Debug.LogException(e);
			}
			db.Dispose();
		}

		private static void ToJson(IDataReader dr, StringBuilder json, string indent) {
			json.Append("{");
			if (!dr.Reset()) { json.Append("}"); return; }
			string key;
			eDataType type;
			object value;
			bool first = true;
			while (dr.Read(out key, out type, out value)) {
				if (first) {
					first = false;
					json.AppendLine();
				} else {
					json.AppendLine(",");
				}
				json.Append(indent);
				json.Append("\t\"");
				json.Append(key);
				json.Append("\":");
				switch (type) {
					case eDataType.Null:
						json.Append("null");
						break;
					case eDataType.Float:
						json.Append(((float)value).ToString("R", CultureInfo.InvariantCulture));
						break;
					case eDataType.Double:
						json.Append(((double)value).ToString("R", CultureInfo.InvariantCulture));
						break;
					case eDataType.Bool:
						json.Append((bool)value ? "true" : "false");
						break;
					case eDataType.String:
						json.Append("\"");
						string str = value as string;
						foreach (char c in str) {
							switch (c) {
								case '"': json.Append("\\\""); break;
								case '\\': json.Append("\\\\"); break;
								case '\b': json.Append("\\b"); break;
								case '\f': json.Append("\\f"); break;
								case '\n': json.Append("\\n"); break;
								case '\r': json.Append("\\r"); break;
								case '\t': json.Append("\\t"); break;
								default: json.Append(c); break;
							}
							if (c == 0) {
								Debug.LogError(str);
								Debug.LogError(str.Length);
							}
						}
						json.Append("\"");
						break;
					case eDataType.Dict:
						ToJson(value as IDataReader, json, indent + "\t");
						break;
					default:
						json.Append(value);
						break;
				}
			}
			json.AppendLine();
			json.Append(indent);
			json.Append("}");
		}

	}

}
