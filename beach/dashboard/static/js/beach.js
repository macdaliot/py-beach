var health_data_mem = {};
var health_data_cpu = {};
var chart_data_mem = [];
var chart_mem;
var chart_data_cpu = [];
var chart_cpu;

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
    }

    chart_mem.render();
    chart_cpu.render();

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

    set_connected( true );
}

function do_refresh()
{
    $.get( '/info', display_data, 'json' ).fail(function(){set_connected(false);})
                                          .always(function(){setTimeout( do_refresh, 5000 );});
}

$(function() {

    chart_mem = new CanvasJS.Chart("health_chart_mem", {
        title : {
            text : "Memory"
        },
        axisX:{
            title: "Time",
        },

         axisY:{
            title: "%",
        },

        data : chart_data_mem
    });

    chart_cpu = new CanvasJS.Chart("health_chart_cpu", {
        title : {
            text : "CPU"
        },
        axisX:{
            title: "Time",
        },

         axisY:{
            title: "%",
        },

        data : chart_data_cpu
    });

	chart_mem.render();
	chart_cpu.render();

    do_refresh();
});
