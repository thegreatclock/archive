using GreatClock.Common.RTS.Collections;
using System;
using System.Collections.Generic;
using System.IO;
using System.Reflection;
using System.Security.Cryptography;
using System.Text;
using System.Text.RegularExpressions;
using UnityEditor;
using UnityEditor.AnimatedValues;
using UnityEngine;

namespace GreatClock.Common.RTS {

	public class RTSDataTypeEditor : EditorWindow {

		[MenuItem("GreatClock/RTS/Open Data Type Editor")]
		static void OpenDataTypeEditor() {
			RTSDataTypeEditor win = GetWindow<RTSDataTypeEditor>("RTS Data Types");
			win.minSize = new Vector2(400f, 300f);
			win.UpdateDataTypes();
			win.Show();
		}

		static MD5 md5_calc = MD5.Create();
		static string GetKey(string key) {
			string str = Application.dataPath + key;
			byte[] bytes = Encoding.UTF8.GetBytes(str);
			bytes = md5_calc.ComputeHash(bytes);
			return BitConverter.ToString(bytes).Replace("-", "");
		}

		static Regex regex_name = new Regex(@"^[_A-Za-z]\w*$");
		static Regex regex_type_and_base = new Regex(@"^[_A-Za-z]\w*(\s*:\s*[_A-Za-z]\w*){0,1}$");
		static Regex regex_ns = new Regex(@"^\w+(\.\w+)*$");

		private class MonoScriptComparer : IComparer<MonoScript> {
			public int Compare(MonoScript x, MonoScript y) {
				return string.Compare(x.name, y.name);
			}
		}

		private static Dictionary<string, Type> s_rts_data_types;
		private static Dictionary<string, Type> rts_data_types {
			get {
				if (s_rts_data_types == null || s_rts_data_types.Count <= 0) {
					Type baseType = typeof(DataNodeBase);
					s_rts_data_types = new Dictionary<string, Type>(16);
					foreach (Assembly assembly in AppDomain.CurrentDomain.GetAssemblies()) {
						foreach (Type type in assembly.GetTypes()) {
							if (!type.IsSubclassOf(baseType)) { continue; }
							s_rts_data_types.Add(type.FullName, type);
						}
					}
				}
				return s_rts_data_types;
			}
		}

		static Type FindCustomDataType(string typeName) {
			foreach (KeyValuePair<string, Type> kv in rts_data_types) {
				if (!kv.Key.EndsWith(typeName)) { continue; }
				int indexDot = kv.Key.Length - typeName.Length - 1;
				if (indexDot < 0 || kv.Key[indexDot] == '.') {
					if (kv.Value == null) { Debug.LogWarning(typeName); }
					return kv.Value;
				}
			}
			return null;
		}

		static bool IsTypeEditing(string typeName, out string ns) {
			ns = null;
			if (s_instance == null) { return false; }
			List<ScriptItem> items = s_instance.mScriptItems;
			foreach (ScriptItem si in items) {
				for (int i = si.TypeCount - 1; i >= 0; i--) {
					TypeItem ti = si.GetTypeItem(i);
					if (ti.TypeName == typeName) {
						ns = si.NameSpace;
						return true;
					}
				}
			}
			return false;
		}

		private class ScriptItem {
			private static Regex s_regex_using = new Regex(@"using\s+((\w+)(\.\w+)*)\s*;");
			private static Regex s_regex_namespace = new Regex(@"namespace\s+((\w+)(\.\w+)*)");
			public AnimBool Unfold;
			public MonoScript Script { get; private set; }
			public string Name { get; private set; }
			private string mNameSpace;
			private List<string> mUsings = new List<string>();
			private List<TypeItem> mTypes = new List<TypeItem>();
			private Action mOnDirty;
			public ScriptItem(MonoScript script, Type[] types) {
				mOnDirty = OnDirty;
				Script = script;
				Name = script.name;
				string code = script.text;
				Match matchUsings = s_regex_using.Match(code);
				while (matchUsings.Success) {
					mUsings.Add(matchUsings.Groups[1].Value);
					matchUsings = matchUsings.NextMatch();
				}
				Match matchNS = s_regex_namespace.Match(code);
				NameSpace = matchNS.Success ? matchNS.Groups[1].Value : "";
				foreach (Type type in types) {
					mTypes.Add(new TypeItem(mOnDirty, type));
				}
				Dirty = false;
			}
			public bool Dirty { get; set; }
			private void OnDirty() { Dirty = true; }
			public string NameSpace {
				get { return mNameSpace; }
				set {
					if (mNameSpace == value) { return; }
					mNameSpace = value;
					IsNameSpaceValid = string.IsNullOrEmpty(mNameSpace) || regex_ns.IsMatch(mNameSpace);
					Dirty = true;
				}
			}
			public bool IsNameSpaceValid { get; private set; }
			public int TypeCount { get { return mTypes.Count; } }
			public TypeItem GetTypeItem(int i) {
				if (i < 0 || i >= mTypes.Count) { return null; }
				return mTypes[i];
			}
			public bool InsertType(int i) {
				if (i < 0 || i > mTypes.Count) { return false; }
				string typeName = "NewTypeName";
				int n = 1;
				while (FindCustomDataType(typeName) != null) {
					typeName = "NewTypeName" + n;
					n++;
				}
				TypeItem ti = new TypeItem(mOnDirty, typeName);
				if (i == mTypes.Count) { mTypes.Add(ti); } else { mTypes.Insert(i, ti); }
				return true;
			}
			public bool RemoveType(int i) {
				if (i < 0 || i >= mTypes.Count) { return false; }
				mTypes.RemoveAt(i);
				Dirty = true;
				return true;
			}
			public string UpdateCode() {
				Dirty = false;
				List<string> usings = new List<string>();
				usings.Add(typeof(RTSSerializeFieldAttribute).Namespace);
				usings.Add(typeof(IList<>).Namespace);
				StringBuilder code = new StringBuilder();
				foreach (TypeItem ti in mTypes) {
					string typeName = string.IsNullOrEmpty(mNameSpace) ? ti.TypeName : string.Concat(mNameSpace, ".", ti.TypeName);
					if (!string.IsNullOrEmpty(ti.BaseType)) {
						Type bt = FindCustomDataType(ti.BaseType);
						if (bt != null) {
							string ns = bt.Namespace;
							if (!string.IsNullOrEmpty(ns) && !usings.Contains(ns)) { usings.Add(ns); }
						}
					}
					code.AppendLine(string.Format("// ** {0} ** //", typeName));
					int n = ti.FieldCount;
					for (int i = 0; i < n; i++) {
						FieldItem fi = ti.GetFieldItem(i);
						string ft = fi.Type;
						if (ft.EndsWith("[]")) {
							string listNS = typeof(RTSList<>).Namespace;
							if (!usings.Contains(listNS)) { usings.Add(listNS); }
							ft = ft.Substring(0, ft.Length - 2);
						} else if (ft.EndsWith("<>")) {
							string dictNS = typeof(RTSDict<>).Namespace;
							if (!usings.Contains(dictNS)) { usings.Add(dictNS); }
							ft = ft.Substring(0, ft.Length - 2);
						}
						if (fi.IsValidBaseType) {
							continue;
						}
						string ns = null;
						Type customFieldType = FindCustomDataType(ft);
						if (customFieldType == null) {
							if (!IsTypeEditing(ft, out ns)) {
								return string.Format("Fail to find type '{0}' !", ft);
							}
						} else {
							ns = customFieldType.Namespace;
						}
						if (!string.IsNullOrEmpty(ns) && !usings.Contains(ns)) { usings.Add(ns); }
					}
				}
				code.AppendLine();
				usings.Remove(mNameSpace);
				usings.Sort();
				foreach (string us in usings) {
					if (string.IsNullOrEmpty(us)) { continue; }
					code.AppendLine(string.Format("using {0};", us));
				}
				code.AppendLine();
				string indent = "";
				if (!string.IsNullOrEmpty(mNameSpace)) {
					code.AppendLine(string.Format("namespace {0} {{", mNameSpace));
					code.AppendLine();
					indent = "\t";
				}
				foreach (TypeItem ti in mTypes) {
					code.AppendLine(string.Format("{0}public class {1} : {2} {{", indent, ti.TypeName, string.IsNullOrEmpty(ti.BaseType) ? "DataNodeBase" : ti.BaseType));
					code.AppendLine();
					int n = ti.FieldCount;
					List<string> toStringFormats = new List<string>(n);
					List<string> toStringItems = new List<string>(n);
					for (int i = 0; i < n; i++) {
						FieldItem fi = ti.GetFieldItem(i);
						string ft = fi.Type;
						string fn = fi.Name;
						code.AppendLine(string.Format("{0}\t[RTSSerializeField(\"{1}\")]", indent, fn));
						toStringFormats.Add(string.Concat(fn, ":{", i, "}"));
						if (ft.EndsWith("[]")) {
							ft = ft.Substring(0, ft.Length - 2);
							code.AppendLine(string.Format("{0}\tprivate RTSList<{1}> _{2}_ = null;", indent, ft, fn));
							code.AppendLine(string.Format("{0}\tprivate byte _{1}_state_ = 0;", indent, fn));
							code.AppendLine(string.Format("{0}\tpublic RTSList<{1}> {2} {{", indent, ft, fn));
							code.AppendLine(string.Format("{0}\t\tget {{ return _{1}_; }}", indent, fn));
							code.AppendLine(string.Format("{0}\t\tset {{", indent));
							code.AppendLine(string.Format("{0}\t\t\tif (_{1}_ == value) {{ return; }}", indent, fn));
							code.AppendLine(string.Format("{0}\t\t\t_{1}_state_ = 2;", indent, fn));
							code.AppendLine(string.Format("{0}\t\t\tRTSList<{1}> prev = _{2}_;", indent, ft, fn));
							code.AppendLine(string.Format("{0}\t\t\tif (prev != null) {{ prev.onModify = null; }}", indent));
							code.AppendLine(string.Format("{0}\t\t\t_{1}_ = value;", indent, fn));
							code.AppendLine(string.Format("{0}\t\t\t_{1}_.onModify = (IRTSList list, int i) => {{", indent, fn));
							code.AppendLine(string.Format("{0}\t\t\t\tif (i < 0) {{", indent));
							code.AppendLine(string.Format("{0}\t\t\t\t\t_{1}_state_ = 2;", indent, fn));
							code.AppendLine(string.Format("{0}\t\t\t\t\tonModify?.Invoke(this);", indent, fn));
							code.AppendLine(string.Format("{0}\t\t\t\t}} else {{", indent));
							code.AppendLine(string.Format("{0}\t\t\t\t\tif (_{1}_state_ <= 0) {{ _{1}_state_ = 1; }}", indent, fn));
							code.AppendLine(string.Format("{0}\t\t\t\t\tonModify?.Invoke(this);", indent, fn));
							code.AppendLine(string.Format("{0}\t\t\t\t}}", indent));
							code.AppendLine(string.Format("{0}\t\t\t}};", indent));
							code.AppendLine(string.Format("{0}\t\t\tonModify?.Invoke(this);", indent, fn));
							code.AppendLine(string.Format("{0}\t\t}}", indent));
							code.AppendLine(string.Format("{0}\t}}", indent));
							code.AppendLine();
							toStringItems.Add(string.Format("{0} == null ? \"null\" : {0}.ToString()", fn));
						} else if (ft.EndsWith("<>")) {
							ft = ft.Substring(0, ft.Length - 2);
							code.AppendLine(string.Format("{0}\tprivate RTSDict<{1}> _{2}_ = null;", indent, ft, fn));
							code.AppendLine(string.Format("{0}\tprivate byte _{1}_state_ = 0;", indent, fn));
							code.AppendLine(string.Format("{0}\tpublic RTSDict<{1}> {2} {{", indent, ft, fn));
							code.AppendLine(string.Format("{0}\t\tget {{ return _{1}_; }}", indent, fn));
							code.AppendLine(string.Format("{0}\t\tset {{", indent));
							code.AppendLine(string.Format("{0}\t\t\tif (_{1}_ == value) {{ return; }}", indent, fn));
							code.AppendLine(string.Format("{0}\t\t\t_{1}_state_ = 2;", indent, fn));
							code.AppendLine(string.Format("{0}\t\t\tRTSDict<{1}> prev = _{2}_;", indent, ft, fn));
							code.AppendLine(string.Format("{0}\t\t\tif (prev != null) {{ prev.onModify = null; }}", indent));
							code.AppendLine(string.Format("{0}\t\t\t_{1}_ = value;", indent, fn));
							code.AppendLine(string.Format("{0}\t\t\t_{1}_.onModify = (IRTSDict dict, string k) => {{", indent, fi.Name));
							code.AppendLine(string.Format("{0}\t\t\t\tif (k == null) {{", indent));
							code.AppendLine(string.Format("{0}\t\t\t\t\t_{1}_state_ = 2;", indent, fn));
							code.AppendLine(string.Format("{0}\t\t\t\t\tonModify?.Invoke(this);", indent, fi.Name));
							code.AppendLine(string.Format("{0}\t\t\t\t}} else {{", indent));
							code.AppendLine(string.Format("{0}\t\t\t\t\tif (_{1}_state_ <= 0) {{ _{1}_state_ = 1; }}", indent, fn));
							code.AppendLine(string.Format("{0}\t\t\t\t\tonModify?.Invoke(this);", indent, fi.Name));
							code.AppendLine(string.Format("{0}\t\t\t\t}}", indent));
							code.AppendLine(string.Format("{0}\t\t\t}};", indent));
							code.AppendLine(string.Format("{0}\t\t\tonModify?.Invoke(this);", indent, fn));
							code.AppendLine(string.Format("{0}\t\t}}", indent));
							code.AppendLine(string.Format("{0}\t}}", indent));
							code.AppendLine();
							toStringItems.Add(string.Format("{0} == null ? \"null\" : {0}.ToString()", fn));
						} else {
							code.AppendLine(string.Format("{0}\tprivate {1} _{2}_;", indent, ft, fn));
							code.AppendLine(string.Format("{0}\tprivate byte _{1}_state_ = 0;", indent, fn));
							if (fi.IsValidBaseType) {
								code.AppendLine(string.Format("{0}\tpublic {1} {2} {{", indent, ft, fn));
								code.AppendLine(string.Format("{0}\t\tget {{ return _{1}_; }}", indent, fn));
								code.AppendLine(string.Format("{0}\t\tset {{ if (_{2}_ == value) {{ return; }} _{2}_state_ = 2; {1} prev = _{2}_; _{2}_ = value; onModify?.Invoke(this); }}", indent, ft, fn));
								code.AppendLine(string.Format("{0}\t}}", indent));
								toStringItems.Add(ft == "string" ? string.Format("{0} == null ? \"null\" : {0}.ToString()", fn) : fn);
							} else {
								code.AppendLine(string.Format("{0}\tpublic {1} {2} {{", indent, ft, fn));
								code.AppendLine(string.Format("{0}\t\tget {{ return _{1}_; }}", indent, fn));
								code.AppendLine(string.Format("{0}\t\tset {{", indent));
								code.AppendLine(string.Format("{0}\t\t\tif (_{1}_ == value) {{ return; }}", indent, fn));
								code.AppendLine(string.Format("{0}\t\t\t_{1}_state_ = 2;", indent, fn));
								code.AppendLine(string.Format("{0}\t\t\t{1} prev = _{2}_;", indent, ft, fn));
								code.AppendLine(string.Format("{0}\t\t\tif (prev != null) {{ prev.onModify = null; }}", indent));
								code.AppendLine(string.Format("{0}\t\t\t_{1}_ = value;", indent, fn));
								code.AppendLine(string.Format("{0}\t\t\t_{1}_.onModify = (DataNodeBase data) => {{ if (_{1}_state_ <= 0) {{ _{1}_state_ = 1; }} onModify?.Invoke(this); }};", indent, fn));
								code.AppendLine(string.Format("{0}\t\t\tonModify?.Invoke(this);", indent, fn));
								code.AppendLine(string.Format("{0}\t\t}}", indent));
								code.AppendLine(string.Format("{0}\t}}", indent));
								toStringItems.Add(string.Format("{0} == null ? \"null\" : {0}.ToString()", fn));
							}
							code.AppendLine();
						}
					}
					code.AppendLine(string.Format("{0}\tprotected override void OnReset() {{", indent));
					for (int i = 0; i < n; i++) {
						FieldItem fi = ti.GetFieldItem(i);
						string ft = fi.Type;
						string fn = fi.Name;
						if (ft.EndsWith("[]") || ft.EndsWith("<>") || !fi.IsValidBaseType) {
							code.AppendLine(string.Format("{0}\t\tif (_{1}_state_ != 0) {{ _{1}_state_ = 0; IDataLoggable loggable = _{1}_; if (loggable != null) {{ loggable.Reset(); }} }}", indent, fn));
						} else {
							code.AppendLine(string.Format("{0}\t\tif (_{1}_state_ != 0) {{ _{1}_state_ = 0; }}", indent, fn));
						}
					}
					if (!string.IsNullOrEmpty(ti.BaseType)) {
						code.AppendLine(string.Format("{0}\t\tbase.OnReset();", indent));
					}
					code.AppendLine(string.Format("{0}\t}}", indent));
					code.AppendLine();
					code.AppendLine(string.Format("{0}\tprotected override void OnCollectChangeLogs(string parent, IList<DataLog> logs) {{", indent));
					for (int i = 0; i < n; i++) {
						FieldItem fi = ti.GetFieldItem(i);
						string ft = fi.Type;
						string fn = fi.Name;
						if (ft.EndsWith("[]") || ft.EndsWith("<>") || !fi.IsValidBaseType) {
							code.AppendLine(string.Format("{0}\t\tif (_{1}_state_ != 0) {{", indent, fn));
							code.AppendLine(string.Format("{0}\t\t\tif (_{1}_state_ == 1) {{", indent, fn));
							code.AppendLine(string.Format("{0}\t\t\t\tIDataLoggable loggable = _{1}_;", indent, fn));
							code.AppendLine(string.Format("{0}\t\t\t\tif (loggable != null) {{ loggable.CollectChangeLogs(string.IsNullOrEmpty(parent) ? \"{1}\" : (parent + \".{1}\"), logs); }}", indent, fn));
							code.AppendLine(string.Format("{0}\t\t\t}} else {{", indent));
							code.AppendLine(string.Format("{0}\t\t\t\tlogs.Add(DataLog.Modify(parent, \"{1}\", _{1}_));", indent, fn));
							code.AppendLine(string.Format("{0}\t\t\t}}", indent));
							code.AppendLine(string.Format("{0}\t\t}}", indent));
						} else {
							code.AppendLine(string.Format("{0}\t\tif (_{1}_state_ != 0) {{ logs.Add(DataLog.Modify(parent, \"{1}\", _{1}_)); }}", indent, fn));
						}
					}
					if (!string.IsNullOrEmpty(ti.BaseType)) {
						code.AppendLine(string.Format("{0}\t\tbase.OnCollectChangeLogs(parent, logs);", indent));
					}
					code.AppendLine(string.Format("{0}\t}}", indent));
					code.AppendLine();
					code.AppendLine(string.Format("{0}\tpublic override string ToString() {{", indent));
					if (!string.IsNullOrEmpty(ti.BaseType)) {
						code.Append(indent);
						code.AppendLine("\t\tstring bs = base.ToString();");
						code.Append(indent);
						code.AppendLine("\t\tint index = bs.IndexOf('{');");
						toStringFormats.Add(string.Concat("{", n, "}"));
						toStringItems.Add("bs.Substring(index + 1, bs.Length - index - 2)");
					}
					code.Append(indent);
					code.Append("\t\treturn string.Format(\"[");
					code.Append(ti.TypeName);
					code.Append("]{{");
					code.Append(string.Join(", ", toStringFormats.ToArray()));
					code.AppendLine("}}\",");
					code.Append(indent);
					code.Append("\t\t\t");
					code.Append(string.Join(", ", toStringItems.ToArray()));
					code.AppendLine(");");
					code.AppendLine(indent + "\t}");
					code.AppendLine();
					code.AppendLine(indent + "}");
					code.AppendLine();
				}
				if (!string.IsNullOrEmpty(mNameSpace)) {
					code.AppendLine("}");
				}
				string path = AssetDatabase.GetAssetPath(Script);
				File.WriteAllText(path, code.ToString(), Encoding.UTF8);
				return null;
			}
		}

		public class TypeItem {
			public readonly Type Type;
			private Action mOnDirty;
			private List<FieldItem> mFields = new List<FieldItem>();
			public TypeItem(Action onDirty, Type type) {
				Type = type;
				Name = typeof(DataNodeBase).Equals(type.BaseType) ? type.Name : (type.Name + " : " + type.BaseType.Name);
				foreach (FieldInfo field in type.GetFields(BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Instance)) {
					RTSSerializeFieldAttribute attr = field.GetCustomAttribute<RTSSerializeFieldAttribute>();
					if (attr == null) { continue; }
					mFields.Add(new FieldItem(onDirty, field.FieldType, attr.Name));
				}
				mOnDirty = onDirty;
			}
			public TypeItem(Action onDirty, string name) {
				Name = name;
				mFields.Add(new FieldItem(onDirty, typeof(int), "id"));
				mFields.Add(new FieldItem(onDirty, typeof(string), "name"));
				mOnDirty = onDirty;
			}
			private string mName;
			public string Name {
				get {
					return mName;
				}
				set {
					if (mName == value) { return; }
					mName = value;
					IsTypeNameValid = regex_type_and_base.IsMatch(mName);
					if (IsTypeNameValid) {
						string[] split = mName.Split(':');
						if (split.Length != 2) {
							TypeName = mName;
							BaseType = null;
						} else {
							TypeName = split[0].Trim();
							BaseType = split[1].Trim();
							string ns;
							IsTypeNameValid = FindCustomDataType(BaseType) != null || IsTypeEditing(BaseType, out ns);
						}
					}
					if (mOnDirty != null) { mOnDirty(); }
				}
			}
			public bool IsTypeNameValid { get; private set; }
			public string TypeName { get; private set; }
			public string BaseType { get; private set; }
			public int FieldCount { get { return mFields.Count; } }
			public FieldItem GetFieldItem(int i) {
				if (i < 0 || i >= mFields.Count) { return null; }
				return mFields[i];
			}
			public bool InsertField(int i) {
				if (i < 0 || i > mFields.Count) { return false; }
				string fn = "new_field";
				int n = 1;
				while (true) {
					bool flag = true;
					foreach (FieldItem field in mFields) {
						if (field.Name == fn) { flag = false; break; }
					}
					if (flag) { break; }
					fn = "new_field" + n;
					n++;
				}
				FieldItem fi = new FieldItem(mOnDirty, typeof(int), fn);
				if (i == mFields.Count) { mFields.Add(fi); } else { mFields.Insert(i, fi); }
				if (mOnDirty != null) { mOnDirty(); }
				return true;
			}
			public bool RemoveField(int i) {
				if (i < 0 || i >= mFields.Count) { return false; }
				mFields.RemoveAt(i);
				if (mOnDirty != null) { mOnDirty(); }
				return true;
			}
		}

		public class FieldItem {
			private static HashSet<string> s_valid_types;
			private static HashSet<string> valid_types {
				get {
					if (s_valid_types == null || s_valid_types.Count <= 0) {
						s_valid_types = new HashSet<string>();
						s_valid_types.Add("byte");
						s_valid_types.Add("sbyte");
						s_valid_types.Add("short");
						s_valid_types.Add("ushort");
						s_valid_types.Add("int");
						s_valid_types.Add("uint");
						s_valid_types.Add("long");
						s_valid_types.Add("ulong");
						s_valid_types.Add("float");
						s_valid_types.Add("double");
						s_valid_types.Add("string");
						s_valid_types.Add("bool");
					}
					return s_valid_types;
				}
			}
			private Action mOnDirty;
			public FieldItem(Action onDirty, Type type, string name) {
				Type t = type;
				bool isArray = false;
				bool isDict = false;
				if (t.IsGenericType) {
					if (t.GetGenericTypeDefinition().Equals(typeof(RTSList<>))) {
						t = t.GetGenericArguments()[0];
						isArray = true;
					} else if (t.GetGenericTypeDefinition().Equals(typeof(RTSDict<>))) {
						t = t.GetGenericArguments()[0];
						isDict = true;
					}
				}
				string tn = "";
				switch (t.FullName) {
					case "System.Byte": tn = "byte"; break;
					case "System.SByte": tn = "sbyte"; break;
					case "System.Int16": tn = "short"; break;
					case "System.UInt16": tn = "ushort"; break;
					case "System.Int32": tn = "int"; break;
					case "System.UInt32": tn = "uint"; break;
					case "System.Int64": tn = "long"; break;
					case "System.UInt64": tn = "ulong"; break;
					case "System.Single": tn = "float"; break;
					case "System.Double": tn = "double"; break;
					case "System.String": tn = "string"; break;
					case "System.Boolean": tn = "bool"; break;
					default:
						tn = t.Name;
						break;
				}
				string ts = tn;
				if (isArray) { ts = tn + "[]"; } else if (isDict) { ts = tn + "<>"; }
				Type = ts;
				Name = name;
				mOnDirty = onDirty;
			}
			public bool IsValidBaseType {
				get {
					string tt = mType;
					if (tt.EndsWith("[]") || tt.EndsWith("<>")) { tt = tt.Substring(0, tt.Length - 2); }
					return valid_types.Contains(tt);
				}
			}
			private string mType;
			public string Type {
				get {
					return mType;
				}
				set {
					if (mType == value) { return; }
					mType = value;
					string tt = mType;
					if (tt.EndsWith("[]") || tt.EndsWith("<>")) { tt = tt.Substring(0, tt.Length - 2); }
					IsTypeValid = true;
					while (true) {
						if (valid_types.Contains(tt)) { break; }
						if (FindCustomDataType(tt) != null) { break; }
						string ns;
						if (IsTypeEditing(tt, out ns)) { break; }
						IsTypeValid = false;
						break;
					}
					if (mOnDirty != null) { mOnDirty(); }
				}
			}
			public bool IsTypeValid { get; private set; }
			private string mName;
			public string Name {
				get {
					return mName;
				}
				set {
					if (mName == value) { return; }
					mName = value;
					IsNameValid = regex_name.IsMatch(mName);
					if (mOnDirty != null) { mOnDirty(); }
				}
			}
			public bool IsNameValid { get; private set; }
		}

		private static RTSDataTypeEditor s_instance;

		private void SaveUnfoldState() {
			StringBuilder content = new StringBuilder();
			foreach (ScriptItem si in mScriptItems) {
				if (!si.Unfold.target) {
					string path = AssetDatabase.GetAssetPath(si.Script);
					content.Append(AssetDatabase.AssetPathToGUID(path));
					content.Append("::");
				}
			}
			EditorPrefs.SetString(GetKey("RTS_DATA_UNFOLD"), content.ToString());
		}

		private bool mToUpdate = false;
		private List<ScriptItem> mScriptItems = new List<ScriptItem>();

		private void UpdateDataTypes() {
			mScriptItems.Clear();
			SortedList<MonoScript, Type[]> dataTypes = new SortedList<MonoScript, Type[]>(32, new MonoScriptComparer());
			Regex reg = new Regex(@"// \*\* ((\w+)(\.\w+)*) \*\* //");
			Dictionary<string, Type> types = rts_data_types;
			string[] scripts = AssetDatabase.FindAssets("t:MonoScript");
			List<Type> ts = new List<Type>(8);
			foreach (string script in scripts) {
				string path = AssetDatabase.GUIDToAssetPath(script);
				MonoScript ms = AssetDatabase.LoadAssetAtPath<MonoScript>(path);
				Match match = reg.Match(ms.text);
				ts.Clear();
				while (match.Success) {
					string name = match.Groups[1].Value;
					Type t;
					if (types.TryGetValue(name, out t)) { ts.Add(t); }
					match = match.NextMatch();
				}
				if (ts.Count <= 0) { continue; }
				dataTypes.Add(ms, ts.ToArray());
				ts.Clear();
			}
			string saved = EditorPrefs.GetString(GetKey("RTS_DATA_UNFOLD"), "");
			List<string> guids = new List<string>(saved.Split(new string[] { "::" }, StringSplitOptions.RemoveEmptyEntries));
			foreach (KeyValuePair<MonoScript, Type[]> kv in dataTypes) {
				ScriptItem si = new ScriptItem(kv.Key, kv.Value);
				string path = AssetDatabase.GetAssetPath(si.Script);
				bool fold = guids.Contains(AssetDatabase.AssetPathToGUID(path));
				si.Unfold = new AnimBool(!fold, Repaint);
				mScriptItems.Add(si);
			}
		}

		void OnEnable() {
			s_instance = this;
		}

		void OnDisable() {
			s_instance = null;
		}

		private Vector2 mScroll;
		private string mSearch;
		void OnGUI() {
			bool isCompiling = EditorApplication.isCompiling;
			if (isCompiling || mScriptItems.Count <= 0) { mToUpdate = true; }
			if (!isCompiling && mToUpdate) {
				mToUpdate = false;
				UpdateDataTypes();
			}
			EditorGUI.BeginDisabledGroup(isCompiling);
			EditorGUILayout.BeginHorizontal();
			mSearch = GUILayout.TextField(mSearch, "SearchTextField");
			if (GUILayout.Button("", "SearchCancelButton")) { mSearch = null; }
			EditorGUILayout.EndHorizontal();
			float width = (position.width - 224f) * 0.5f;
			mScroll = EditorGUILayout.BeginScrollView(mScroll, false, false, GUILayout.ExpandHeight(true));
			int n = mScriptItems.Count;
			int count = -1;
			for (int i = 0; i < n; i++) {
				ScriptItem si = mScriptItems[i];
				bool flag = true;
				if (!string.IsNullOrEmpty(mSearch)) {
					while (true) {
						if (si.Name.IndexOf(mSearch, StringComparison.OrdinalIgnoreCase) >= 0) { break; }
						bool ok = false;
						for (int j = 0, jmax = si.TypeCount; j < jmax; j++) {
							if (si.GetTypeItem(j).TypeName.IndexOf(mSearch, StringComparison.OrdinalIgnoreCase) >= 0) { ok = true; break; }
						}
						if (ok) { break; }
						flag = false;
						break;
					}
				}
				if (!flag) { continue; }
				count++;
				Color cachedColor = GUI.color;
				Color zebra = (count & 1) == 0 ? GUI.backgroundColor : new Color(0.8f, 0.8f, 0.8f);
				GUI.backgroundColor = zebra;
				EditorGUILayout.BeginVertical("CN Box", GUILayout.MinHeight(10f));
				GUI.backgroundColor = new Color(0.5f, 0.5f, 0.5f);
				EditorGUILayout.BeginHorizontal("CN Box", GUILayout.MinHeight(10f));
				GUI.backgroundColor = cachedColor;
				if (GUILayout.Button(si.Unfold.target ? "\u25BC" : "\u25BA", "WhiteMiniLabel", GUILayout.Width(16f))) {
					si.Unfold.target = !si.Unfold.target;
					SaveUnfoldState();
				}
				cachedColor = GUI.backgroundColor;
				GUI.backgroundColor = new Color(0.5f, 0.5f, 0.5f);
				EditorGUILayout.ObjectField(si.Script, typeof(MonoScript), false);
				GUI.backgroundColor = cachedColor;
				GUILayout.FlexibleSpace();
				EditorGUI.BeginDisabledGroup(!si.Dirty);
				if (GUILayout.Button("Update", GUILayout.Width(64f))) {
					string error = si.UpdateCode();
					if (string.IsNullOrEmpty(error)) {
						AssetDatabase.Refresh();
						mToUpdate = true;
					} else {
						EditorUtility.DisplayDialog("Rts Data Types", "Failed when generating code !\n" + error, "OK");
					}
				}
				EditorGUI.EndDisabledGroup();
				EditorGUILayout.EndHorizontal();
				if (EditorGUILayout.BeginFadeGroup(si.Unfold.faded)) {
					EditorGUILayout.BeginHorizontal();
					GUILayout.Label("NameSpace", GUILayout.Width(80f));
					cachedColor = GUI.backgroundColor;
					if (!si.IsNameSpaceValid) { GUI.backgroundColor = Color.red; }
					si.NameSpace = EditorGUILayout.DelayedTextField(si.NameSpace);
					GUI.backgroundColor = cachedColor;
					EditorGUILayout.EndHorizontal();
					int m = si.TypeCount;
					int toInsertType = -1;
					int toRemoveType = -1;
					for (int j = 0; j < m; j++) {
						cachedColor = GUI.backgroundColor;
						GUI.backgroundColor = zebra;
						EditorGUILayout.BeginVertical("CN Box", GUILayout.MinHeight(10f));
						GUI.backgroundColor = cachedColor;
						TypeItem ti = si.GetTypeItem(j);
						EditorGUILayout.BeginHorizontal();
						cachedColor = GUI.color;
						if (!ti.IsTypeNameValid) { GUI.color = Color.red; }
						ti.Name = EditorGUILayout.DelayedTextField(ti.Name, (GUIStyle)"BoldLabel", GUILayout.ExpandWidth(false), GUILayout.MinWidth(10f));
						GUI.color = cachedColor;
						GUILayout.FlexibleSpace();
						if (GUILayout.Button("+", "minibutton", GUILayout.Width(18f))) { toInsertType = j; }
						if (GUILayout.Button("x", "minibutton", GUILayout.Width(18f))) { toRemoveType = j; }
						EditorGUILayout.EndHorizontal();
						GUILayout.Space(6f);
						int p = ti.FieldCount;
						int toInsertField = -1;
						int toRemoveField = -1;
						for (int k = 0; k < p; k++) {
							EditorGUILayout.BeginHorizontal();
							FieldItem fi = ti.GetFieldItem(k);
							GUILayout.Label("Field Type:", GUILayout.Width(72f));
							cachedColor = GUI.backgroundColor;
							if (!fi.IsTypeValid) { GUI.backgroundColor = Color.red; }
							fi.Type = EditorGUILayout.DelayedTextField(fi.Type, GUILayout.Width(width));
							GUI.backgroundColor = cachedColor;
							GUILayout.FlexibleSpace();
							GUILayout.Label("Field Name:", GUILayout.Width(72f));
							cachedColor = GUI.backgroundColor;
							if (!fi.IsNameValid) { GUI.backgroundColor = Color.red; }
							fi.Name = EditorGUILayout.DelayedTextField(fi.Name, GUILayout.Width(width));
							GUI.backgroundColor = cachedColor;
							if (GUILayout.Button("+", "minibutton", GUILayout.Width(18f))) { toInsertField = k; }
							if (GUILayout.Button("x", "minibutton", GUILayout.Width(18f))) { toRemoveField = k; }
							EditorGUILayout.EndHorizontal();
						}
						EditorGUILayout.BeginHorizontal();
						GUILayout.FlexibleSpace();
						if (GUILayout.Button("Create New Field", "minibutton", GUILayout.Width(100f))) { toInsertField = p; }
						EditorGUILayout.EndHorizontal();
						if (toInsertField >= 0) { ti.InsertField(toInsertField); }
						if (toRemoveField >= 0) { ti.RemoveField(toRemoveField); }
						EditorGUILayout.EndVertical();
					}
					cachedColor = GUI.backgroundColor;
					GUI.backgroundColor = zebra;
					EditorGUILayout.BeginHorizontal("CN Box", GUILayout.MinHeight(10f));
					GUI.backgroundColor = cachedColor;
					if (GUILayout.Button("New Data Type", "minibutton", GUILayout.Width(100f))) { toInsertType = m; }
					GUILayout.FlexibleSpace();
					EditorGUILayout.EndHorizontal();
					if (toInsertType >= 0) { si.InsertType(toInsertType); }
					if (toRemoveType >= 0) { si.RemoveType(toRemoveType); }
					GUILayout.Space(2f);
				}
				EditorGUILayout.EndFadeGroup();
				EditorGUILayout.EndVertical();
			}
			EditorGUILayout.EndScrollView();

			if (GUILayout.Button("Create New Data Script")) {
				string path = EditorUtility.SaveFilePanelInProject("New RTS Data File", "NewRTSData", "cs", "");
				if (!string.IsNullOrEmpty(path)) {
					string error = CreateNewDataFileAtPath(path);
					if (!string.IsNullOrEmpty(error)) {
						EditorUtility.DisplayDialog("New RTS Data File",
							string.Format("Failed in creating RTS Data file at '{0}' !\n{1}", path, error), "OK");
					} else {
						AssetDatabase.Refresh();
					}
				}
			}
			EditorGUI.EndDisabledGroup();
		}

		private string CreateNewDataFileAtPath(string path) {
			if (string.IsNullOrEmpty(path)) { return null; }
			string className = Path.GetFileNameWithoutExtension(path);
			if (!regex_name.IsMatch(className)) { return "Illegal class name : " + className; }
			StringBuilder code = new StringBuilder();
			code.AppendLine(string.Format("// ** {0} ** //", className));
			code.AppendLine();
			code.AppendLine(string.Format("using {0};", typeof(RTSSerializeFieldAttribute).Namespace));
			code.AppendLine("using System.Collections.Generic;");
			code.AppendLine();
			code.AppendLine(string.Format("public class {0} : DataNodeBase {{", className));
			code.AppendLine();
			code.AppendLine("\t[RTSSerializeField(\"id\")]");
			code.AppendLine("\tprivate int _id_;");
			code.AppendLine("\tpublic int id { get { return _id_; } set { _id_ = value; } }");
			code.AppendLine();
			code.AppendLine("\t[RTSSerializeField(\"name\")]");
			code.AppendLine("\tprivate string _name_;");
			code.AppendLine("\tpublic string name { get { return _name_; } set { _name_ = value; } }");
			code.AppendLine();
			code.AppendLine("\tprotected override void OnReset() { }");
			code.AppendLine();
			code.AppendLine("\tprotected override void OnCollectChangeLogs(string parent, IList<DataLog> logs) { }");
			code.AppendLine();
			code.AppendLine("}");
			File.WriteAllText(path, code.ToString(), Encoding.UTF8);
			return null;
		}

	}

}
