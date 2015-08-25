/**
 * Created by Sven on 15.08.2015.
 */
var terminalServer = function(listen_addr, listen_port) {

    this.ws = require('nodejs-websocket');
    this.mysql = require('mysql');
    this.sql_conn = null;

    this.server = null;
    this.addr = listen_addr;
    this.port = listen_port;

    console.log('Initialisiere terminalServer on '+this.addr+':'+this.port);

    console.log('Datenbankverbindung wird hergestellt');
    this.sql_conn = this.mysql.createConnection({
        host     : WM_MYSQL_HOST,
	    port	 : WM_MYSQL_PORT,
        user     : WM_MYSQL_USER,
        password : WM_MYSQL_PASSWORD,
        database : WM_MYSQL_DATABASE
    });

    this.debugCounter = 0;

    this.start = function() {
        var me = this;


        this.server = this.ws.createServer(function (conn) {
            me.newClient(conn);

            conn.on("text", function (str) {

            })

            conn.on("close", function (code, reason) {
                me.log("Connection closed " + code+ " - "+reason);
            })

        }, function() {
            this.log('sadsad');
        }).listen(this.port, this.addr);

    }

    this.newClient = function(connection) {
        var me = this;
        var terminal_id = this.parseRequest(connection.path);
        if(terminal_id == null) {
            this.log('Verbindung abgewiesen - keine TerminalID übertragen.');
            connection.close();
            return;
        }
        //console.log('Neue Verbindung von Terminal '+terminal_id +' [Aktive Verbindungen: '+this.server.connections.length+'] prÃ¼fe Daten');

        connection.terminal_id = terminal_id;
        connection.init_data_loaded = false;
        connection.init_data = false;
        connection.display_name = null;
        connection.display_description = null;
        connection.display_typ = null;
        connection.display_keyData = null;
        connection.send_last_state = null;

        this.sql_conn.query('SELECT name, beschreibung, titel, untertitel, typ, dataKey FROM display_views WHERE status=\'enable\' AND id='+terminal_id, function(err, rows, fields) {
            if(rows.length == 0) {
                me.log('Verbindung abgewiesen - keine Berechtigung');
                connection.close();
            } else {
                connection.display_name = rows[0].name;
                connection.display_description = rows[0].beschreibung;
                connection.display_typ = rows[0].typ;
                connection.display_titel = rows[0].titel;
                connection.display_untertitel = rows[0].untertitel;
                connection.display_keyData = rows[0].dataKey;
                connection.init_data_loaded = true;
                connection.send_last_state = null;
                me.log('Neue Verbindung von Terminal '+terminal_id +'[T:'+connection.display_typ+' Name:'+connection.display_name+'] [Aktive Verbindungen: '+me.server.connections.length+'] prÃ¼fe Daten');
                if(connection.display_typ == "verladung") {
                    me.initialFirstTimeVerladung(connection, connection.display_keyData);
                } else {
                    me.initialFirstTime(connection);
                }
            }
        });
    }

    this.sendToClient = function(client, Output) {
        Output.timestamp = Math.round((new Date()).getTime() / 1000);
        client.sendText(JSON.stringify(Output));
    }

    this.initialFirstTime = function(connection) {
        if(connection.init_data) { return false; }

        var Output = new Object();
        Output.action = 'init';
        Output.data = [
            {key:'display_name', data: connection.display_name},
            {key:'display_titel', data: connection.display_titel},
            {key:'display_untertitel', data: connection.display_untertitel}
        ];

        this.sendToClient(connection, Output);
        this.sendUpdatedData();
    }

    this.initialFirstTimeVerladung = function(connection, tor) {
        var me = this;
        if(connection.init_data) { return false; }

        var first_qry = "SELECT a.verladung_id, b.bemerkung, a.abzug, b.tor, b.hinweise FROM live_current_day a LEFT JOIN verladung b ON a.verladung_id=b.id LEFT JOIN terminal_tore c ON b.tor=c.id WHERE a.status='pending' AND c.nummer='"+tor+"' ORDER BY a.abzug LIMIT 1";
        this.sql_conn.query({sql:first_qry,typeCast: true}, function(err, rows, fields) {
            if(rows.length > 0) {
                //me.log('Tor: '+tor+': '+rows[0].verladung_id);
                var Output = new Object();
                Output.action = 'init';
                Output.data = [
                    {key:'display_name', data: connection.display_name},
                    {key:'display_titel', data: connection.display_titel},
                    {key:'display_untertitel', data: connection.display_untertitel},
                    {key:'verladung_id', data: rows[0].verladung_id},
                    {key:'tor_id', data: rows[0].tor},
                    {key:'bemerkung', data: rows[0].bemerkung },
                    {key:'hinweise', data: rows[0].hinweise},
                    {key:'abzug', data: rows[0].abzug}
                ];

                connection.last_verladung_id = rows[0].verladung_id;

                me.sendToClient(connection, Output);
                me.sendUpdatedData();
            }
        });


    }

    this.sendUpdatedData = function() {
        for(var i = 0; i < this.server.connections.length; i++) {
            var client = this.server.connections[i];
            var qry_extend = null;
            if(client.display_typ == "controller" && client.display_keyData == 0) {
                qry_extend = "";
            } else if(client.display_typ == "info" && client.display_keyData == 0) {
                //Info-Display-Data global
                qry_extend = "";
            } else if(client.display_typ == "info" && client.display_keyData > 0) {
                //Info-Display-Data Verladetor
                qry_extend = "AND d.nummer>="+client.display_keyData+" AND d.nummer<="+(parseInt(client.display_keyData)+100)+"" ;
            } else if(client.display_typ == "verladung") {
                //Verlade-Display - Verladetor.
                this.sendVerladungsDaten(client, client.display_keyData);
            }

            if(qry_extend !== null) {
                this.sendItemsByQuery(client, qry_extend);
            }
        }
    }

    this.sendVerladungsDaten = function(client, tor) {
        var me = this;
        var qry_data = 0;
        var date = new Date('1970-01-01 06:00:00');
        qry_data = date.getFullYear()+'-'+(date.getMonth()+1)+'-'+date.getDate()+' '+date.getHours()+':'+date.getMinutes()+':'+date.getSeconds();
        if(client.send_last_state !== null) {
            date = new Date(client.send_last_state);
            qry_data = date.getFullYear()+'-'+(date.getMonth()+1)+'-'+date.getDate()+' '+date.getHours()+':'+date.getMinutes()+':'+date.getSeconds();
        }

        var first_qry = "SELECT a.verladung_id, b.bemerkung FROM live_current_day a LEFT JOIN verladung b ON a.verladung_id=b.id LEFT JOIN terminal_tore c ON b.tor=c.id WHERE a.status='pending' AND c.nummer='"+tor+"' ORDER BY a.abzug LIMIT 1";
        this.sql_conn.query({sql:first_qry,typeCast: true}, function(err, rows, fields) {
            if(rows.length > 0) {
                var verlade_id = rows[0].verladung_id;
                if(client.last_verladung_id != verlade_id) {
                    me.initialFirstTimeVerladung(client, tor);
                } else {
                    client.last_verladung_id = verlade_id;
                    var second_qry = "SELECT * FROM live_current_day a LEFT JOIN verladung_vkhs b ON a.vkh_id=b.vkh_id LEFT JOIN vkhs c ON a.vkh_id=c.id WHERE  a.lastchange>'" + qry_data + "' AND b.verladung_id=" + verlade_id + " ORDER BY b.reihenfolge ASC";
                    me.sql_conn.query({sql:second_qry,typeCast: true}, function(err1, rows1, fields1) {
                        for (var i = 0; i < rows1.length; i++) {
                            if(me.compareDate(date, rows1[i].lastchange) < 0) {
                                date = rows1[i].lastchange;
                                client.send_last_state = rows1[i].lastchange.toString();
                            }
                            var Output = new Object();
                            Output.action = 'item';
                            Output.data = rows1[i];
                            me.sendToClient(client, Output);
                        }
                    });
                }
            }
        });
    }

    this.sendItemsByQuery = function(client, qry_extend) {
        var me = this;
        var qry_data = 0;
        var date = new Date('1970-01-01 06:00:00');
        qry_data = date.getFullYear()+'-'+(date.getMonth()+1)+'-'+date.getDate()+' '+date.getHours()+':'+date.getMinutes()+':'+date.getSeconds();
        if(client.send_last_state !== null) {
            date = new Date(client.send_last_state);
            qry_data = date.getFullYear()+'-'+(date.getMonth()+1)+'-'+date.getDate()+' '+date.getHours()+':'+date.getMinutes()+':'+date.getSeconds();
        }

        var qry = "SELECT a.id, a.verladung_id, a.datum,a.ansatz, a.abzug, a.vkh_id, a.komm_akl, a.komm_hrl, a.komm_kvl, a.komm_al, a.transport_bft, a.transport_kvl";
        qry += ", a.transport_al, a.absort_akl,  a.absort_hrl, a.verladung, a.close_komm_akl, a.close_komm_hrl, a.close_komm_kvl, a.close_komm_al, a.close_transport_bft";
        qry += ", a.close_transport_kvl, a.close_transport_al, a.close_absort_akl, a.close_absort_hrl, a.close_verladung, a.lastchange, a.status, b.name, b.kennung, d.nummer";

        qry += " FROM live_current_day a LEFT JOIN vkhs b ON a.vkh_id=b.id LEFT JOIN verladung c ON a.verladung_id=c.id LEFT JOIN terminal_tore d ON c.tor=d.id WHERE 1=1 "+qry_extend+" AND a.lastchange>'"+qry_data+"' ORDER BY a.abzug ASC";
        this.sql_conn.query({sql:qry,typeCast: true}, function(err, rows, fields) {
            //console.log('Es wurden insgesamt '+rows.length+' geÃ¤nderte EintrÃ¤ge gefunden. ['+qry_data+'] ('+qry+')');
            if(rows.length > 0) {

                for(var i = 0; i < rows.length; i++) {
                    if(me.compareDate(date, rows[i].lastchange) < 0) {
                        date = rows[i].lastchange;
                        client.send_last_state = rows[i].lastchange.toString();
                    }


                    var Output = new Object();
                    Output.action = 'item';
                    Output.data = rows[i];
                    me.sendToClient(client, Output);
                }
            }
        });
    }

    this.compareDate = function(a,b) {
        return (
            isFinite(a=this.convertDate(a).valueOf()) &&
            isFinite(b=this.convertDate(b).valueOf()) ?
            (a>b)-(a<b) :
                NaN
        );
    }

    this.convertDate = function(d) {
        // Converts the date in d to a date-object. The input can be:
        //   a date object: returned without modification
        //  an array      : Interpreted as [year,month,day]. NOTE: month is 0-11.
        //   a number     : Interpreted as number of milliseconds
        //                  since 1 Jan 1970 (a timestamp)
        //   a string     : Any format supported by the javascript engine, like
        //                  "YYYY/MM/DD", "MM/DD/YYYY", "Jan 31 2009" etc.
        //  an object     : Interpreted as an object with year, month and date
        //                  attributes.  **NOTE** month is 0-11.
        return (
            d.constructor === Date ? d :
                d.constructor === Array ? new Date(d[0],d[1],d[2]) :
                    d.constructor === Number ? new Date(d) :
                        d.constructor === String ? new Date(d) :
                            typeof d === "object" ? new Date(d.year,d.month,d.date) :
                                NaN
        );
    }

    this.parseRequest = function(request) {
        var terminal_id = null;

        var conn_str_path_parts = request.split("/");
        for(var i = 0; i < conn_str_path_parts.length; i++) {
            if(conn_str_path_parts[i] == "terminalId") {
                terminal_id = parseInt(conn_str_path_parts[i+1]);
            }
        }
        return terminal_id;
    }

    this.stop = function() {
        this.log('Server wird beendet.');
        this.server.close();
        this.mysql.close();
    }

    this.error = function(e, broken){

        //console.log(e);
    }

    this.broadcast = function broadcast(data) {
        var me = this;
        if(this.server != null && this.server.connections.length > 0) {
            this.server.connections.forEach(function each(client) {
                var Output = new Object();
                Output.action = 'ping';
                Output.data = data;
                me.sendToClient(client, Output);
                //client.sendText(data);
            });
        }
    };

    this.broadcastTime = function() {
        this.debugCounter++;
        this.broadcast(this.debugCounter);
    }


    this.log = function(message) {
        var date = new Date();

        var h = date.getHours();
        var m = date.getMinutes();
        var s = date.getSeconds();

        if(h < 10) {
            h = "0"+h;
        }

        if(m < 10) {
            m = "0"+m;
        }

        if(s < 10) {
            s = "0"+s;
        }

        var output = h+':'+m+':'+s+' '+message;


        console.log(output);
    }


}

process.on('uncaughtException', function (er) {
    var spacer = '                                                          ###';
    console.log('#####   ###   #####   ###   #####   ###   #####   ###   #####');
    console.log('###    Fehler: '+ er.message+spacer.substr(er.message.length+15));
    console.log('#####   ###   #####   ###   #####   ###   #####   ###   #####');
    console.error('Stacktrace: '+er.stack);
    process.exit(1);
});

var ipaddress = process.env.OPENSHIFT_NODEJS_IP || "127.0.0.1";
var port      = process.env.OPENSHIFT_NODEJS_PORT || 8080;

var DisplayServer = new terminalServer(ipaddress , port);
DisplayServer.start();


setInterval(function() {
    //console.log('Sende neue Items');
    DisplayServer.sendUpdatedData();
}, 1000);

