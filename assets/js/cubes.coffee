getConnectionDataOnCube = undefined

getConnectionDataOnCube = (cubeId, sourceId, updateTreeUrl, dataUrl, closingUrl) ->
  info =
    'cube_id': cubeId
    'source_id': sourceId
  resp = getConnectionData(dataUrl, closingUrl)
  resp.then ->
    $.get updateTreeUrl, info, (res) ->
      $('#dbTables').slideToggle()
      $.each res.tables, (i, t) ->
        table = $('#' + t)
        $('#' + t + '>input[type="checkbox"]').attr 'checked', true
        return
      drawTables res.data
      $('#data-table-headers').html ''
      $('#data-table-headers').append colsHeaders(data: res.data)
      $('#button-allToLeft').removeClass 'disabled'
      $('#button-allToRight').removeClass 'disabled'
      return
    return
  return