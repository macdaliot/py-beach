var health_data_mem = {};
var health_data_cpu = {};
var health_data_qps = {};
var chart_data_mem = [];
var chart_mem;
var chart_data_cpu = [];
var chart_cpu;
var chart_data_qps = [];
var chart_qps;
var max_timeline_values = ((60/10)*60*12);


function avg_array( arr )
{
    var total = 0;

    for( var i in arr )
    {
        total += arr[ i ];
    }

    return total / arr.length;
}

function set_connected( isConnected )
{
    if( !isConnected )
    {
        $('#status_indicator').removeClass( 'glyphicon-ok' );
        $('#status_indicator').addClass( 'glyphicon-remove' );
        $('#status_indicator').css( 'color', 'red' );
    }
    else
    {
        $('#status_indicator').removeClass( 'glyphicon-remove' );
        $('#status_indicator').addClass( 'glyphicon-ok' );
        $('#status_indicator').css( 'color', 'green' );
    }
}

function display_data( data )
{
    $('#num_nodes').text( data.n_nodes );
    $('#num_actors').text( data.n_actors );
    $('#num_realms').text( data.n_realms );
    $('#num_cats').text( data.n_cats );

    for( var k in data.health )
    {
        var node_name = k;
        var node_info = data.health[ k ];

        if( node_info == null ){ continue; }

        if( !(node_name in health_data_mem) )
        {
            health_data_mem[ node_name ] = { legendText: node_name,
                                             type: 'line',
                                             showInLegend: true,
                                             xValueType: "dateTime",
                                             dataPoints: [] };
            chart_data_mem.push( health_data_mem[ node_name ] );
        }
        health_data_mem[ node_name ].dataPoints.push( { x: (new Date).getTime(),
                                                        y: node_info.mem } );
        if( health_data_mem[ node_name ].dataPoints.length > max_timeline_values )
        {
            health_data_mem[ node_name ].dataPoints.shift();
        }

        if( !(node_name in health_data_cpu) )
        {
            health_data_cpu[ node_name ] = { legendText: node_name,
                                             type: 'line',
                                             showInLegend: true,
                                             xValueType: "dateTime",
                                             dataPoints: [] };
            chart_data_cpu.push( health_data_cpu[ node_name ] );
        }
        health_data_cpu[ node_name ].dataPoints.push( { x: (new Date).getTime(),
                                                        y: avg_array( node_info.cpu ) } );
        if( health_data_cpu[ node_name ].dataPoints.length > max_timeline_values )
        {
            health_data_cpu[ node_name ].dataPoints.shift();
        }
    }

    var tmp_qps = {};

    for( var k in data.load )
    {
        var actor_id = k;
        var actor_name = data.actor_mtd[ k ];
        var loads = data.load[ k ];

        if( !(actor_name in health_data_qps) )
        {
            health_data_qps[ actor_name ] = { legendText: actor_name,
                                              type: 'line',
                                              showInLegend: true,
                                              xValueType: "dateTime",
                                              dataPoints: [] };
            chart_data_qps.push( health_data_qps[ actor_name ] );
        }
        if( !( actor_name in tmp_qps ) )
        {
            tmp_qps[ actor_name ] = 0;
        }
        tmp_qps[ actor_name ] += loads[ 3 ];
    }

    for( var k in tmp_qps )
    {
        health_data_qps[ k ].dataPoints.push( { x: (new Date).getTime(),
                                                         y: tmp_qps[ k ] } );
        if( health_data_qps[ k ].dataPoints.length > max_timeline_values )
        {
            health_data_qps[ k ].dataPoints.shift();
        }
    }

    chart_mem.render();
    chart_cpu.render();
    chart_qps.render();

    $('#realm_dir').empty();
    for( var k in data.dir.realms )
    {
        var realm = k;
        var cats = data.dir.realms[ k ];
        for( var c in cats )
        {
            var category_name = c;
            var cat_num = Object.keys( cats[ c ] ).length;
            $('#realm_dir').append( $("<tr>").append( $("<td>").text( realm ) )
                                             .append( $("<td>").text( category_name ) )
                                             .append( $("<td>").text( cat_num ) ) );
        }
    }

    $('#actors_info').empty();
    for( var k in data.load )
    {
        var actor_id = k;
        var actor_name = data.actor_mtd[ k ];
        var loads = data.load[ k ];
        var entry = $("<tr>").append( $("<td>").text( actor_id )
                                               .attr( 'nowrap', 'nowrap' )
                                               .append( $("<br>") )
                                               .append( $("<i>").text( actor_name ) ) )
                             .append( $("<td>").text( loads[ 0 ] ) )
                             .append( $("<td>").text( loads[ 1 ] ) )
                             .append( $("<td>").text( loads[ 3 ] ) );

        if( loads[ 0 ] == 0 )
        {
            entry.css( 'background-color', 'red' )
        }
        $('#actors_info').append( entry );

        entry = $("<tr>").append( $("<br>") )
                         .append( $("<td>").attr( "colspan", "2" )
                                           .append( $("<b>").text( "Pending transactions:" ) )
                                           .append( $("<pre>").text( JSON.stringify( loads[ 2 ],
                                                                                     null, 4 ) ) ) );
        $('#actors_info').append( entry );
    }

    set_connected( true );
}

function do_refresh()
{
    $.get( '/info', display_data, 'json' ).fail(function(){set_connected(false);})
                                          .always(function(){setTimeout( do_refresh, 10000 );});
}

$(function() {

    chart_mem = new CanvasJS.Chart("health_chart_mem", {
        zoomEnabled : true,
        panEnabled : true,
        title : {
            text : "Memory"
        },
        axisX:{
            title: "Time",
        },
        toolTip: {
            content: "{x}<br/> <span style='\"'color: {color};'\"'>{legendText}:</span> {y}",
        },
         axisY:{
            title: "%",
            maximum: 100,
        },

        data : chart_data_mem
    });

    chart_cpu = new CanvasJS.Chart("health_chart_cpu", {
        zoomEnabled : true,
        panEnabled : true,
        title : {
            text : "CPU"
        },
        axisX:{
            title: "Time",
        },
        toolTip: {
            content: "{x}<br/> <span style='\"'color: {color};'\"'>{legendText}:</span> {y}",
        },
         axisY:{
            title: "%",
            maximum: 100,
        },

        data : chart_data_cpu
    });

    chart_qps = new CanvasJS.Chart("health_chart_qps", {
        zoomEnabled : true,
        panEnabled : true,
        title : {
            text : "QPS"
        },
        axisX:{
            title: "Time",
        },
        toolTip: {
            content: "{x}<br/> <span style='\"'color: {color};'\"'>{legendText}:</span> {y}",
        },
         axisY:{
            title: "Queries",
        },

        data : chart_data_qps
    });

	chart_mem.render();
	chart_cpu.render();
    chart_qps.render();

    do_refresh();
});
