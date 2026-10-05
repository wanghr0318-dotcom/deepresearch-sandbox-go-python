package jcs

import (
	"encoding/json"
	"errors"
	"math"
	"testing"
)

// RFC 8785 §3.2.2 的示例：数字、字符串转义与字面量。
func TestCanonicalRFCExample(t *testing.T) {
	in := `{
	  "numbers": [333333333.33333329, 1E30, 4.50, 2e-3, 0.000000000000000000000000001],
	  "string": "\u20ac$\u000F\u000aA'\u0042\u0022\u005c\\\"\/",
	  "literals": [null, true, false]
	}`
	want := `{"literals":[null,true,false],"numbers":[333333333.3333333,1e+30,4.5,0.002,1e-27],"string":"€$\u000f\nA'B\"\\\\\"/"}`
	got, err := Canonical(json.RawMessage(in))
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != want {
		t.Fatalf("得到 %s\n期望 %s", got, want)
	}
}

// RFC 8785 §3.2.3 的排序示例：按 UTF-16 码元而非码点排序（U+1F600 的代理项 D83D 小于 U+FB33）。
func TestCanonicalSortsByUTF16(t *testing.T) {
	in := `{"\u20ac":"Euro Sign","\r":"Carriage Return","\ufb33":"Hebrew Letter Dalet With Dagesh","1":"One",` +
		`"\ud83d\ude00":"Emoji: Grinning Face","\u0080":"Control","\u00f6":"Latin Small Letter O With Diaeresis"}`
	want := "{\"\\r\":\"Carriage Return\",\"1\":\"One\",\"\u0080\":\"Control\",\"ö\":\"Latin Small Letter O With Diaeresis\"," +
		"\"€\":\"Euro Sign\",\"😀\":\"Emoji: Grinning Face\",\"דּ\":\"Hebrew Letter Dalet With Dagesh\"}"
	got, err := Canonical(json.RawMessage(in))
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != want {
		t.Fatalf("得到 %s\n期望 %s", got, want)
	}
}

// RFC 8785 附录 B 的双精度数向量（ECMAScript Number.prototype.toString）。
func TestFormatNumberVectors(t *testing.T) {
	cases := map[uint64]string{
		0x0000000000000000: "0",
		0x8000000000000000: "0",
		0x0000000000000001: "5e-324",
		0x8000000000000001: "-5e-324",
		0x7fefffffffffffff: "1.7976931348623157e+308",
		0xffefffffffffffff: "-1.7976931348623157e+308",
		0x4340000000000000: "9007199254740992",
		0xc340000000000000: "-9007199254740992",
		0x4430000000000000: "295147905179352830000",
		0x44b52d02c7e14af5: "9.999999999999997e+22",
		0x44b52d02c7e14af6: "1e+23",
		0x44b52d02c7e14af7: "1.0000000000000001e+23",
		0x444b1ae4d6e2ef4e: "999999999999999700000",
		0x444b1ae4d6e2ef4f: "999999999999999900000",
		0x444b1ae4d6e2ef50: "1e+21",
		0x3eb0c6f7a0b5ed8c: "9.999999999999997e-7",
		0x3eb0c6f7a0b5ed8d: "0.000001",
		0x41b3de4355555553: "333333333.3333332",
		0x41b3de4355555554: "333333333.33333325",
		0x41b3de4355555555: "333333333.3333333",
		0x41b3de4355555556: "333333333.3333334",
		0x41b3de4355555557: "333333333.33333343",
		0xbecbf647612f3696: "-0.0000033333333333333333",
		0x43143ff3c1cb0959: "1424953923781206.2",
	}
	for bits, want := range cases {
		if got := FormatNumber(math.Float64frombits(bits)); got != want {
			t.Errorf("%016x：得到 %s，期望 %s", bits, got, want)
		}
	}
}

// 重复属性名（含转义后相同的名字、嵌套对象中的重复）被拒绝；不同对象中的同名属性不算重复。
func TestCanonicalRejectsDuplicateKeys(t *testing.T) {
	for _, in := range []string{`{"a":1,"a":2}`, `{"a":1,"\u0061":1}`, `{"x":[{"b":1,"b":1}]}`} {
		if _, err := Canonical(json.RawMessage(in)); !errors.Is(err, ErrDuplicateKey) {
			t.Errorf("%s：期望 ErrDuplicateKey，得到 %v", in, err)
		}
	}
	if _, err := Canonical(json.RawMessage(`{"a":{"a":1},"b":{"a":2}}`)); err != nil {
		t.Fatalf("不同对象中的同名属性：%v", err)
	}
}

// 不是 I-JSON 的输入被拒绝：孤立代理项、超出双精度范围的数字、非法 UTF-8。
func TestCanonicalRejectsNonIJSON(t *testing.T) {
	for _, in := range []string{`"\ud800"`, `"\udc00\ud800"`, `1e400`, "\"\xff\""} {
		if _, err := Canonical(json.RawMessage(in)); !errors.Is(err, ErrInvalid) {
			t.Errorf("%q：期望 ErrInvalid，得到 %v", in, err)
		}
	}
}

// 结构体经 encoding/json 编码后规范化：字段顺序与空白不影响结果，HTML 字符不转义。
func TestCanonicalStructAndRaw(t *testing.T) {
	type s struct {
		B string          `json:"b"`
		A json.RawMessage `json:"a"`
	}
	got, err := Canonical(s{B: "<&>", A: json.RawMessage(`{ "z" : 1.50, "y" : [ ] }`)})
	if err != nil {
		t.Fatal(err)
	}
	if want := `{"a":{"y":[],"z":1.5},"b":"<&>"}`; string(got) != want {
		t.Fatalf("得到 %s，期望 %s", got, want)
	}
}
