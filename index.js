const mysql = require('mysql')
const assert = require('assert')

class MysqlConn
{
    constructor(cfg)
    {
        this.conn = mysql.createConnection(cfg);
        this.conn.connect((e) => {
            assert.equal(e, null);
        });
    }

    execQuery(conn, sql, params, callback)
    {
        console.debug(sql, params);

        conn.query(sql, params, (e, res) => {
            conn.end((e) => { assert.equal(e, null); });
            if (e) {
                console.error('[execQuery] - :' + e);
            } else {
                callback(res);
            }
        });
        callback();
    }

    query(sql, params, callback)
    {
        this.execQuery(this.conn, sql, params, callback);
    }

    createWhere(params, where, where_limits)
    {
        var count = 0;
        var sql = "";
        if (where) {
            sql += " WHERE "
            for (let k in where) {
                params.push(where[k]);
                sql += "`" + k + "` = ?";

                // add string: 'AND/OR'
                if (where_limits[count]) {
                    sql += " " + where_limits[count].toUpperCase() + " ";
                }
                ++count;
            }
        }

        return {
            params:params,
            sql:sql
        }
    }

    /**
     * 
     * @param {tblname} db table's name. 
     * @param {data} Object
     */
    insert(tblname, data)
    {
        var conn = this.conn;
        var len = Object.keys(data).length;
        var execQuery = this.execQuery;

        function onExecute(callback)
        {
            var counter = 0;
            var params = [];
            var sql = "INSERT INTO `" + tblname + "`(";
            var values = ") VALUES ("
            for (let k in data) {
                params.push(data[k]);
                sql += "`" + k + "`";
                values += "?";

                // add string: ','
                if (++counter < len) {
                    sql += ",";
                    values += ",";
                }
            }

            sql += values + ")";
            
            execQuery(conn, sql, params, callback);
        }

        return {
            execute:onExecute
        }
    }

    /**
     * 
     * @param {tblname} db table's name 
     * @param {where} Object 
     * @param {where_limits} 'or' or 'and' in Array  
     */
    delete(tblname, where, where_limits=[])
    {
        var conn = this.conn;
        var execQuery = this.execQuery;
        var createWhere = this.createWhere;

        function onExecute(callback)
        {
            var params = [];
            var sql = "DELETE FROM `" + tblname + "`";
            var res = createWhere(params, where, where_limits);
            params = res.params;
            sql += res.sql;

            execQuery(conn, sql, params, callback);
        }

        return {
            execute:onExecute
        }
    }

    /**
     * 
     * @param {*} tblname db table's name
     * @param {*} fields Array
     * @param {*} where Object
     * @param {*} where_limits 'or ' or 'and' in Array
     */
    select(tblname, fields, where, where_limits=[])
    {
        var conn = this.conn;
        var execQuery = this.execQuery;
        var createWhere = this.createWhere;

        function onExecute(callback)
        {
            var count = 0;
            var sql = "SELECT ";
            for (let i = 0; i < fields.length; i++) {
                sql += "`" + fields[i] + "`";
    
                if (++count < fields.length)
                    sql += ", ";
            }
    
            var params = [];
            sql += " FROM `" + tblname + "`";
            var res = createWhere(params, where, where_limits);
            params = res.params;
            sql += res.sql;

            execQuery(conn, sql, params, callback);
        }

        return {
            execute:onExecute
        }
    }

    /**
     * 
     * @param {*} tblname db table's name
     * @param {*} sets Object
     * @param {*} where Object
     * @param {*} where_limits 'or' or 'and' in Array
     */
    update(tblname, sets, where, where_limits=[])
    {
        var conn = this.conn;
        var execQuery = this.execQuery;
        var createWhere = this.createWhere;

        function onExecute(callback)
        {
            var count = 0;
            var params = [];
            var len = Object.keys(sets).length;
            var sql = "UPDATE `" + tblname + "` SET "
            for (let k in sets) {
                params.push(sets[k]);
                sql += "`" + k + "` = ? ";

                // add string ','
                if (++count < len) {
                    sql += ", ";
                }
            }

            var res = createWhere(params, where, where_limits);
            params = res.params;
            sql += res.sql;

            execQuery(conn, sql, params, callback);
        }

        return {
            execute:onExecute
        }
    }
}

module.exports = MysqlConn;
