var getConnectionDataOnCube;


getConnectionDataOnCube = function(cubeId, sourceId, updateTreeUrl, dataUrl, closingUrl) {
  var info, resp;
  info = {
    'cube_id': cubeId,
    'source_id': sourceId
  };
  resp = getConnectionData(dataUrl, closingUrl);
  resp.then(function() {
    $.get(updateTreeUrl, info, function(res) {
      $('#dbTables').slideToggle();
      $.each(res.tables, function(i, t) {
        var table;
        table = $('#' + t);
        $('#' + t + '>input[type="checkbox"]').attr('checked', true);
      });
      drawTables(res.data);
      $('#data-table-headers').html('');
      $('#data-table-headers').append(colsHeaders({
        data: res.data
      }));
      $('#button-allToLeft').removeClass('disabled');
      $('#button-allToRight').removeClass('disabled');
    });
  });
};