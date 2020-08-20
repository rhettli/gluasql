package gluasql

import (
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"github.com/jinzhu/gorm"
	"github.com/junhsieh/goexamples/fieldbinding/fieldbinding"
	util "github.com/rhettli/gluasql/util"
	"github.com/yuin/gopher-lua"
	"reflect"
	"time"
)

func Preload(L *lua.LState) {
	//L.PreloadModule("mysql", mysql.Loader)
	L.PreloadModule("gorm", Loader)
	//db, _ := gorm.Open("postgres", "host=myhost port=myport user=gorm dbname=gorm password=mypassword")
	//defer db.Close()
}

func newFn(L *lua.LState) int {
	client := &Client{}
	ud := L.NewUserData()
	ud.Value = client
	L.SetMetatable(ud, L.GetTypeMetatable(CLIENT_TYPENAME))
	L.Push(ud)
	return 1
}

var exports = map[string]lua.LGFunction{
	"new": newFn,
}

func Loader(L *lua.LState) int {
	mod := L.SetFuncs(L.NewTable(), exports)
	L.Push(mod)

	L.SetField(mod, "_DEBUG", lua.LBool(true))
	L.SetField(mod, "_VERSION", lua.LString("1.0"))

	registerClientType(L)

	return 1
}

const (
	CLIENT_TYPENAME = "gorm{client}"
)

func registerClientType(L *lua.LState) {
	mt := L.NewTypeMetatable(CLIENT_TYPENAME)
	L.SetField(mt, "__index", L.SetFuncs(L.NewTable(), clientMethods))
}

// Client sqlite3
type Client struct {
	DB      *gorm.DB
	Timeout time.Duration
}

var clientMethods = map[string]lua.LGFunction{
	"open":        clientOpenMethod,
	"set_timeout": clientSetTimeoutMethod,
	"close":       clientCloseMethod,
	"query":       clientQueryMethod,
}

func checkClient(L *lua.LState) *Client {
	ud := L.CheckUserData(1)
	if v, ok := ud.Value.(*Client); ok {
		return v
	}
	L.ArgError(1, "client expected")
	return nil
}

func clientSetTimeoutMethod(L *lua.LState) int {
	client := checkClient(L)
	timeout := L.ToInt64(2) // timeout (in ms)

	client.Timeout = time.Millisecond * time.Duration(timeout)
	return 0
}

func clientCloseMethod(L *lua.LState) int {
	client := checkClient(L)

	if client.DB == nil {
		L.Push(lua.LBool(true))
		return 1
	}

	err := client.DB.Close()
	// always clean
	client.DB = nil
	if err != nil {
		L.Push(lua.LBool(false))
		L.Push(lua.LString(err.Error()))
		return 2
	}

	L.Push(lua.LBool(true))
	return 1
}

func clientOpenMethod(L *lua.LState) int {
	client := checkClient(L)

	sqlType := L.ToString(2) //sqlite3 mysql postgres
	// mysql "user:password@/dbname?charset=utf8&parseTime=True&loc=Local
	// postgres host=myhost port=myport user=gorm dbname=gorm password=mypassword

	conStr := L.ToString(3)

	//tb := util.GetValue(L, 3)
	//options, ok := tb.(map[string]interface{})

	var err error
	client.DB, err = gorm.Open(sqlType, conStr)
	if err != nil {
		L.Push(lua.LBool(false))
		L.Push(lua.LString(err.Error()))
		return 2
	}

	L.Push(lua.LBool(true))
	return 1
}

func clientQueryMethod(L *lua.LState) int {
	client := checkClient(L)
	query := L.ToString(2)

	if client.DB == nil {
		return 0
	}

	if query == "" {
		L.ArgError(2, "query string required")
		return 0
	}

	fn1 := L.Get(3).(*lua.LFunction) //fn := L.GetGlobal("coro").(*lua.LFunction) /* get function from lua */

	//var result []interface{}
	//client.DB.Raw("SELECT * FROM users WHERE id = ?", 1).Rows()

	//fmt.Println("raw query before:",result)
	//rows, err := client.DB.Raw("SELECT * FROM users WHERE name = ?", "root").Rows()
	rows, err := client.DB.Raw(query).Rows()

	//fmt.Println("raw query:", rows, err)

	if err == nil {
		for rows.Next() {
			fb := fieldbinding.NewFieldBinding()
			cols, err := rows.Columns()
			if err != nil {
				fmt.Println("get sql columns err:", err.Error())
				continue
			}
			fb.PutFields(cols)
			if err := rows.Scan(fb.GetFieldPtrArr()...); err != nil {
				fmt.Println("scan err:", err.Error())
			} else {
				tbRow := util.ToTableFromMap(L, reflect.ValueOf(fb.GetFieldArr()))
				//fmt.Println("scan done:", tbRow)
				co, _ := L.NewThread()
				st, err, values := L.Resume(co, fn1, tbRow)
				if st == lua.ResumeError {
					fmt.Println("yield break(error)", err, values)
					//break
				}
				//for i, lv := range values {
				//	fmt.Printf("%v : %v\n", i, lv)
				//}
				if st != lua.ResumeOK {
					fmt.Println("yield break(err)")
					//break
				}
			}

			//tbRow := util.ToTableFromMap(L, reflect.ValueOf(fb.GetFieldArr()))
			//tb.Append(tbRow)
		}
	} else {
		fmt.Println("query sql err:", err.Error())
	}

	//sec := L.CheckInt(1)
	//caller := L.CheckString(3)
	//async.AsyncRun(func() []lua.LValue {
	//	//time.Sleep(time.Second * time.Duration(sec))
	//	return []lua.LValue{lua.LString(caller)}
	//}, L)

	//rows, err := client.DB.Find(query)
	//if err != nil {
	//	L.Push(lua.LNil)
	//	L.Push(lua.LString(err.Error()))
	//	return 2
	//}
	//defer rows.Close()
	//
	//fb := fieldbinding.NewFieldBinding()
	//cols, err := rows.Columns()
	//if err != nil {
	//	L.Push(lua.LNil)
	//	L.Push(lua.LString(err.Error()))
	//	return 2
	//}
	//
	//fb.PutFields(cols)
	//
	//tb := L.NewTable()
	//for rows.Next() {
	//	if err := rows.Scan(fb.GetFieldPtrArr()...); err != nil {
	//		L.Push(lua.LNil)
	//		L.Push(lua.LString(err.Error()))
	//		return 2
	//	}
	//
	//	tbRow := util.ToTableFromMap(L, reflect.ValueOf(fb.GetFieldArr()))
	//	tb.Append(tbRow)
	//}
	//
	//L.Push(tb)
	return 0
}
