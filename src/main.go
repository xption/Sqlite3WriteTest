package main

import (
	"database/sql"
	"fmt"
	"os"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

func main() {
	for i := 0; i < 10; i++ {
		timeDuration := test(2, 1000)
		fmt.Println("耗时：", timeDuration)
	}
}

func test(typ int, count int) time.Duration {
	// 移除data.bin文件
	err := os.Remove("data.bin")
	if err != nil && !os.IsNotExist(err) {
		panic("移除旧的data.bin文件失败")
	}

	// 打开数据库
	db, err := sql.Open("sqlite3", "data.bin")
	if err != nil {
		panic(err.Error())
	}
	defer db.Close()

	// 创建表
	sqlCreate := "CREATE TABLE IF NOT EXISTS `mst_map_module_meta` (`module_id` INTEGER,`module_name` TEXT,`map_id` INTEGER,`coordinate` TEXT,`interact_coordinate` TEXT,`interact_radius` INTEGER,`radius` INTEGER,`timing` INTEGER,`reveal_fog` INTEGER,`module_type` INTEGER,`block` INTEGER,`mp_id` INTEGER,`type_information` TEXT,`module_image_type` INTEGER,`module_image_id` INTEGER,`already_deblocking_id` INTEGER,`not_deblocking_id` INTEGER,`view_scope` INTEGER,`way_finding_coordinate` TEXT,`deblocking_condition_type` TEXT,`deblocking_value` TEXT,`offset` TEXT,`effect` INTEGER,`action` TEXT,`sound` INTEGER,`logo_height` INTEGER,`minimap_display` INTEGER,`minimap_resources` TEXT,`occurrence_condition` TEXT);"

	_, err = db.Exec(sqlCreate)
	if err != nil {
		fmt.Println("执行 sql 失败:", err.Error())
	}

	// 插入
	switch typ {
	case 0:
		return insert(db, count)
	case 1:
		return insertAsync(db, count)
	case 2:
		return insertAsyncWithTrans(db, count)
	case 3:
		return insertAsyncWithTransAndPrepare(db, count)
	default:
		panic("typ 错误")
	}
}

func insert(db *sql.DB, count int) time.Duration {
	timeBegin := time.Now()

	// 执行插入
	sqlInsert := "INSERT INTO mst_map_module_meta (`module_id`,`module_name`,`map_id`,`coordinate`,`interact_coordinate`,`interact_radius`,`radius`,`timing`,`reveal_fog`,`module_type`,`block`,`mp_id`,`type_information`,`module_image_type`,`module_image_id`,`already_deblocking_id`,`not_deblocking_id`,`view_scope`,`way_finding_coordinate`,`deblocking_condition_type`,`deblocking_value`,`offset`,`effect`,`action`,`sound`,`logo_height`,`minimap_display`,`minimap_resources`,`occurrence_condition`) VALUES (1020001,'返回103',102,'[[5,4,2]]','[5,4]',0,0,1,0,3,0,0,'{\"mapid\":103,\"coordinate\":[4,8]}',1,108,0,0,-1,'','','','',10301,'',0,0,0,'','')"

	for i := 0; i < count; i++ {
		_, err := db.Exec(sqlInsert)
		if err != nil {
			fmt.Println("执行 sql 失败:", err.Error())
		}
	}

	timeEnd := time.Now()
	return timeEnd.Sub(timeBegin)
}

func insertAsync(db *sql.DB, count int) time.Duration {
	// 关闭写同步（提升效率）
	_, err := db.Exec("PRAGMA synchronous = OFF")
	if err != nil {
		panic(err.Error())
	}

	return insert(db, count)
}

func insertAsyncWithTrans(db *sql.DB, count int) time.Duration {
	// 关闭写同步（提升效率）
	_, err := db.Exec("PRAGMA synchronous = OFF")
	if err != nil {
		panic(err.Error())
	}

	// 开启事务（提升效率）
	_, err = db.Exec("BEGIN")
	if err != nil {
		panic(err.Error())
	}

	defer func() {
		_, err = db.Exec("COMMIT")
		if err != nil {
			panic(err.Error())
		}
	}()

	return insert(db, count)
}

func insertAsyncWithTransAndPrepare(db *sql.DB, count int) time.Duration {
	// 关闭写同步（提升效率）
	_, err := db.Exec("PRAGMA synchronous = OFF")
	if err != nil {
		panic(err.Error())
	}

	// 开启事务（提升效率）
	_, err = db.Exec("BEGIN")
	if err != nil {
		panic(err.Error())
	}

	defer func() {
		_, err = db.Exec("COMMIT")
		if err != nil {
			panic(err.Error())
		}
	}()

	timeBegin := time.Now()

	// 执行插入
	sqlInsert := "INSERT INTO mst_map_module_meta (`module_id`,`module_name`,`map_id`,`coordinate`,`interact_coordinate`,`interact_radius`,`radius`,`timing`,`reveal_fog`,`module_type`,`block`,`mp_id`,`type_information`,`module_image_type`,`module_image_id`,`already_deblocking_id`,`not_deblocking_id`,`view_scope`,`way_finding_coordinate`,`deblocking_condition_type`,`deblocking_value`,`offset`,`effect`,`action`,`sound`,`logo_height`,`minimap_display`,`minimap_resources`,`occurrence_condition`) VALUES (1020001,'返回103',102,'[[5,4,2]]','[5,4]',0,0,1,0,3,0,0,'{\"mapid\":103,\"coordinate\":[4,8]}',1,108,0,0,-1,'','','','',10301,'',0,0,0,'','')"

	sqlStmt, err := db.Prepare(sqlInsert)
	if err != nil {
		panic(err.Error())
	}

	for i := 0; i < count; i++ {
		_, err = sqlStmt.Exec()
		if err != nil {
			fmt.Println("执行 sql 失败:", err.Error())
		}
	}

	timeEnd := time.Now()
	return timeEnd.Sub(timeBegin)
}
