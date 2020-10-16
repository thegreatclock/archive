using GreatClock.Common.RTS.Collections;
using System;
using System.Collections;
using System.Globalization;
using System.IO;
using System.Text;

namespace GreatClock.Common.RTS.Utilities {

	public static class RTSJson {

		static Type type_byte = typeof(byte);
		static Type type_sbyte = typeof(sbyte);
		static Type type_short = typeof(short);
		static Type type_ushort = typeof(ushort);
		static Type type_int = typeof(int);
		static Type type_uint = typeof(uint);
		static Type type_long = typeof(long);
		static Type type_ulong = typeof(ulong);
		static Type type_float = typeof(float);
		static Type type_double = typeof(double);
		static Type type_string = typeof(string);

		static Type type_data_node_base = typeof(DataNodeBase);
		static Type type_rts_list_interface = typeof(IRTSList);
		static Type type_rts_dict_interface = typeof(IRTSDict);

		public static T Deserialize<T>(string json) {
			if (string.IsNullOrEmpty(json)) { return default(T); }
			return Parser.Parse<T>(new StringReader(json));
		}

		public static object Deserialize(Type type, string json) {
			if (string.IsNullOrEmpty(json)) { return null; }
			return Parser.Parse(type, new StringReader(json));
		}

		sealed class Parser {
			const string WORD_BREAK = "{}[],:\"";

			public static bool IsWordBreak(char c) {
				return Char.IsWhiteSpace(c) || WORD_BREAK.IndexOf(c) != -1;
			}

			enum TOKEN {
				NONE,
				CURLY_OPEN,
				CURLY_CLOSE,
				SQUARED_OPEN,
				SQUARED_CLOSE,
				COLON,
				COMMA,
				STRING,
				NUMBER,
				TRUE,
				FALSE,
				NULL
			};

			private StringBuilder mTempBuilder = new StringBuilder();
			StringReader json;

			Parser(StringReader json) {
				this.json = json;
			}

			public static T Parse<T>(StringReader json) {
				var instance = new Parser(json);
				return (T)instance.ParseNext(typeof(T));
			}

			public static object Parse(Type type, StringReader json) {
				var instance = new Parser(json);
				return instance.ParseNext(type);
			}

			object ParseObject(Type type) {
				NodeTypeFieldData data = NodeTypeFieldData.GetNodeTypeFieldData(type);
				if (data == null) { return null; }
				object ret = Activator.CreateInstance(type);
				json.Read();
				while (true) {
					switch (NextToken) {
						case TOKEN.NONE:
							return null;
						case TOKEN.COMMA:
							continue;
						case TOKEN.CURLY_CLOSE:
							return ret;
						default:
							string name = ParseString(type_string);
							if (name == null) { return null; }
							if (NextToken != TOKEN.COLON) { return null; }
							json.Read();
							NodeTypeFieldData.FieldData field = data.GetField(name);
							if (field.field == null) { ParseNext(null); break; }
							object value = ParseNext(field.field.FieldType);
							if (field.field.FieldType.IsSubclassOf(type_data_node_base)) {
								if (field.property == null) { break; }
							}
							if (field.property != null) { field.property.SetValue(ret, value); } else { field.field.SetValue(ret, value); }
							break;
					}
				}
			}

			bool ParseObjectArray(IRTSList array, Type type) {
				switch (NextToken) {
					case TOKEN.CURLY_OPEN:
						json.Read();
						while (true) {
							switch (NextToken) {
								case TOKEN.NONE:
									return false;
								case TOKEN.COMMA:
									continue;
								case TOKEN.CURLY_CLOSE:
									return true;
							}
							string name = ParseString(type_string);
							if (name == null) { return false; }
							if (NextToken != TOKEN.COLON) { return false; }
							json.Read();
							object value = ParseNext(type);
							if (!double.IsNaN(RTSListUtil.FromKey(name))) {
								double p = double.Parse(name.Substring(1, name.Length - 1));
								if (array != null) { array.RawAdd(p, value); }
							} else {
								if (array != null) { array.Add(value); }
							}
						}
					case TOKEN.SQUARED_OPEN:
						json.Read();
						while (true) {
							switch (NextToken) {
								case TOKEN.NONE:
									return false;
								case TOKEN.COMMA:
									continue;
								case TOKEN.SQUARED_CLOSE:
									return true;
								default:
									object value = ParseNext(type);
									if (array != null) { array.Add(value); }
									break;
							}
						}
				}
				return false;
			}

			bool ParseObjectDict(IRTSDict dict, Type type) {
				if (NextToken != TOKEN.CURLY_OPEN) { return false; }
				json.Read();
				while (true) {
					switch (NextToken) {
						case TOKEN.NONE:
							return false;
						case TOKEN.COMMA:
							continue;
						case TOKEN.CURLY_CLOSE:
							return true;
					}
					string name = ParseString(type_string);
					if (name == null) { return false; }
					if (NextToken != TOKEN.COLON) { return false; }
					json.Read();
					object value = ParseNext(type);
					if (dict != null && name != "[]") { dict.Add(name, value); }
				}
			}

			object ParseNext(Type type) {
				switch (NextToken) {
					case TOKEN.STRING:
						return ParseString(type);
					case TOKEN.NUMBER:
						return ParseNumber(type);
					case TOKEN.CURLY_OPEN:
						if (type_rts_list_interface.IsAssignableFrom(type)) {
							IRTSList list = Activator.CreateInstance(type) as IRTSList;
							ParseObjectArray(list, type.GetGenericArguments()[0]);
							return list;
						}
						if (type_rts_dict_interface.IsAssignableFrom(type)) {
							IRTSDict dict = Activator.CreateInstance(type) as IRTSDict;
							ParseObjectDict(dict, type.GetGenericArguments()[0]);
							return dict;
						} else {
							return ParseObject(type);
						}
					case TOKEN.SQUARED_OPEN:
						IRTSList array = type != null ? Activator.CreateInstance(type) as IRTSList : null;
						ParseObjectArray(array, type);
						return array;
					case TOKEN.TRUE:
						return true;
					case TOKEN.FALSE:
						return false;
					case TOKEN.NULL:
						return null;
					default:
						return null;
				}
			}

			string ParseString(Type type) {
				mTempBuilder.Clear();
				char c;
				json.Read();
				bool parsing = true;
				while (parsing) {
					if (json.Peek() == -1) {
						parsing = false;
						break;
					}
					c = NextChar;
					switch (c) {
						case '"':
							parsing = false;
							break;
						case '\\':
							if (json.Peek() == -1) {
								parsing = false;
								break;
							}
							c = NextChar;
							switch (c) {
								case '"':
								case '\\':
								case '/':
									mTempBuilder.Append(c);
									break;
								case 'b':
									mTempBuilder.Append('\b');
									break;
								case 'f':
									mTempBuilder.Append('\f');
									break;
								case 'n':
									mTempBuilder.Append('\n');
									break;
								case 'r':
									mTempBuilder.Append('\r');
									break;
								case 't':
									mTempBuilder.Append('\t');
									break;
								case 'u':
									var hex = new char[4];
									for (int i = 0; i < 4; i++) {
										hex[i] = NextChar;
									}
									mTempBuilder.Append((char)Convert.ToInt32(new string(hex), 16));
									break;
							}
							break;
						default:
							mTempBuilder.Append(c);
							break;
					}
				}
				return mTempBuilder.ToString();
			}

			object ParseNumber(Type type) {
				string number = NextWord;
				if (number.IndexOf('.') == -1) {
					if (type.Equals(type_byte)) {
						byte value;
						byte.TryParse(number, out value);
						return value;
					} else if (type.Equals(type_sbyte)) {
						sbyte value;
						sbyte.TryParse(number, out value);
						return value;
					} else if (type.Equals(type_short)) {
						short value;
						short.TryParse(number, out value);
						return value;
					} else if (type.Equals(type_ushort)) {
						ushort value;
						ushort.TryParse(number, out value);
						return value;
					} else if (type.Equals(type_int)) {
						int value;
						int.TryParse(number, out value);
						return value;
					} else if (type.Equals(type_uint)) {
						uint value;
						uint.TryParse(number, out value);
						return value;
					} else if (type.Equals(type_long)) {
						long value;
						long.TryParse(number, out value);
						return value;
					} else if (type.Equals(type_ulong)) {
						ulong value;
						ulong.TryParse(number, out value);
						return value;
					}
					// type not supported
					return null;
				}
				if (type.Equals(type_float)) {
					float value;
					float.TryParse(number, NumberStyles.Float, CultureInfo.InvariantCulture, out value);
					return value;
				} else if (type.Equals(type_double)) {
					double value;
					double.TryParse(number, NumberStyles.Float, CultureInfo.InvariantCulture, out value);
					return value;
				}
				// type not supported
				return null;
			}

			void EatWhitespace() {
				while (Char.IsWhiteSpace(PeekChar)) {
					json.Read();

					if (json.Peek() == -1) {
						break;
					}
				}
			}

			char PeekChar {
				get {
					return Convert.ToChar(json.Peek());
				}
			}

			char NextChar {
				get {
					return Convert.ToChar(json.Read());
				}
			}

			string NextWord {
				get {
					mTempBuilder.Clear();
					while (!IsWordBreak(PeekChar)) {
						mTempBuilder.Append(NextChar);

						if (json.Peek() == -1) {
							break;
						}
					}
					return mTempBuilder.ToString();
				}
			}

			TOKEN NextToken {
				get {
					EatWhitespace();

					if (json.Peek() == -1) {
						return TOKEN.NONE;
					}

					switch (PeekChar) {
						case '{':
							return TOKEN.CURLY_OPEN;
						case '}':
							json.Read();
							return TOKEN.CURLY_CLOSE;
						case '[':
							return TOKEN.SQUARED_OPEN;
						case ']':
							json.Read();
							return TOKEN.SQUARED_CLOSE;
						case ',':
							json.Read();
							return TOKEN.COMMA;
						case '"':
							return TOKEN.STRING;
						case ':':
							return TOKEN.COLON;
						case '0':
						case '1':
						case '2':
						case '3':
						case '4':
						case '5':
						case '6':
						case '7':
						case '8':
						case '9':
						case '-':
							return TOKEN.NUMBER;
					}

					switch (NextWord) {
						case "false":
							return TOKEN.FALSE;
						case "true":
							return TOKEN.TRUE;
						case "null":
							return TOKEN.NULL;
					}

					return TOKEN.NONE;
				}
			}
		}

		public static string Serialize(object obj, bool list2dict) {
			return Serializer.Serialize(obj, list2dict, false);
		}

		public static string Serialize(object obj, bool list2dict, bool perfectPrint) {
			return Serializer.Serialize(obj, list2dict, perfectPrint);
		}

		sealed class Serializer {
			StringBuilder builder;
			string indent;

			Serializer() {
				builder = new StringBuilder();
				indent = "";
			}

			string Json { get { return builder.ToString(); } }

			public static string Serialize(object obj, bool list2dict, bool perfectPrint) {
				var instance = new Serializer();
				instance.SerializeValue(true, obj, list2dict, perfectPrint);
				return instance.Json;
			}

			void SerializeValue(bool first, object value, bool list2dict, bool perfectPrint) {
				IRTSList asList;
				IRTSDict asDict;
				DataNodeBase node;
				string asStr;

				if (value == null) {
					builder.Append("null");
				} else if ((asStr = value as string) != null) {
					SerializeString(asStr);
				} else if (value is bool) {
					builder.Append((bool)value ? "true" : "false");
				} else if ((asList = value as IRTSList) != null) {
					if (perfectPrint && !first) { builder.AppendLine(); }
					SerializeArray(asList, list2dict, perfectPrint);
				} else if ((asDict = value as IRTSDict) != null) {
					if (perfectPrint && !first) { builder.AppendLine(); }
					SerializeDict(asDict, list2dict, perfectPrint);
				} else if ((node = value as DataNodeBase) != null) {
					if (perfectPrint && !first) { builder.AppendLine(); }
					SerializeObject(node, list2dict, perfectPrint);
				} else if (value is char) {
					SerializeString(new string((char)value, 1));
				} else {
					SerializeOther(value);
				}
			}

			void SerializeObject(DataNodeBase obj, bool list2dict, bool perfectPrint) {
				NodeTypeFieldData data = NodeTypeFieldData.GetNodeTypeFieldData(obj.GetType());
				NodeTypeFieldData.FieldData[] fields = data.GetFields();
				bool first = true;

				if (perfectPrint) { builder.Append(indent); }
				builder.Append('{');
				indent = indent + "    ";

				foreach (NodeTypeFieldData.FieldData field in fields) {
					if (!first) { builder.Append(','); }
					if (perfectPrint) {
						builder.AppendLine();
						builder.Append(indent);
					}
					SerializeString(field.name);
					builder.Append(':');
					SerializeValue(false, field.field.GetValue(obj), list2dict, perfectPrint);
					first = false;
				}
				indent = indent.Substring(4);
				if (perfectPrint) {
					builder.AppendLine();
					builder.Append(indent);
				}
				builder.Append('}');
			}

			void SerializeArray(IRTSList anArray, bool list2dict, bool perfectPrint) {
				if (list2dict) {
					if (perfectPrint) { builder.Append(indent); }
					builder.Append("{");
					indent = indent + "    ";
					int i = 0;
					foreach (object obj in anArray) {
						if (i > 0) { builder.Append(','); }
						if (perfectPrint) {
							builder.AppendLine();
							builder.Append(indent);
						}
						builder.Append("\"");
						builder.Append(RTSListUtil.ToKey(anArray.GetPriority(i)));
						builder.Append("\":");
						SerializeValue(false, obj, list2dict, perfectPrint);
						i++;
					}
					indent = indent.Substring(4);
					if (perfectPrint) {
						builder.AppendLine();
						builder.Append(indent);
					}
					builder.Append("}");
				} else {
					if (perfectPrint) { builder.Append(indent); }
					builder.Append('[');
					indent = indent + "    ";
					bool first = true;
					foreach (object obj in anArray) {
						if (!first) { builder.Append(','); }
						if (perfectPrint) {
							builder.AppendLine();
							builder.Append(indent);
						}
						SerializeValue(false, obj, list2dict, perfectPrint);
						first = false;
					}
					indent = indent.Substring(4);
					if (perfectPrint) {
						builder.AppendLine();
						builder.Append(indent);
					}
					builder.Append(']');
				}
			}

			void SerializeDict(IRTSDict dict, bool list2dict, bool perfectPrint) {
				if (perfectPrint) { builder.Append(indent); }
				builder.Append("{");
				indent = indent + "    ";
				int i = 0;
				IDictionaryEnumerator iter = dict.GetEnumerator();
				while (iter.MoveNext()) {
					if (i > 0) { builder.Append(','); }
					if (perfectPrint) {
						builder.AppendLine();
						builder.Append(indent);
					}
					builder.Append("\"");
					builder.Append(iter.Key.ToString());
					builder.Append("\":");
					SerializeValue(false, iter.Value, list2dict, perfectPrint);
					i++;
				}
				indent = indent.Substring(4);
				if (perfectPrint) {
					builder.AppendLine();
					builder.Append(indent);
				}
				builder.Append("}");
			}

			void SerializeString(string str) {
				builder.Append('\"');

				char[] charArray = str.ToCharArray();
				foreach (var c in charArray) {
					switch (c) {
						case '"':
							builder.Append("\\\"");
							break;
						case '\\':
							builder.Append("\\\\");
							break;
						case '\b':
							builder.Append("\\b");
							break;
						case '\f':
							builder.Append("\\f");
							break;
						case '\n':
							builder.Append("\\n");
							break;
						case '\r':
							builder.Append("\\r");
							break;
						case '\t':
							builder.Append("\\t");
							break;
						default:
							int codepoint = Convert.ToInt32(c);
							if ((codepoint >= 32) && (codepoint <= 126)) {
								builder.Append(c);
							} else {
								builder.Append("\\u");
								builder.Append(codepoint.ToString("x4"));
							}
							break;
					}
				}

				builder.Append('\"');
			}

			void SerializeOther(object value) {
				if (value is float) {
					builder.Append(((float)value).ToString("R", CultureInfo.InvariantCulture));
				} else if (value is int
					|| value is uint
					|| value is long
					|| value is sbyte
					|| value is byte
					|| value is short
					|| value is ushort
					|| value is ulong) {
					builder.Append(value.ToString());
				} else if (value is double
					|| value is decimal) {
					builder.Append(Convert.ToDouble(value).ToString("R", CultureInfo.InvariantCulture));
				} else {
					SerializeString(value.ToString());
				}
			}
		}

	}

}
