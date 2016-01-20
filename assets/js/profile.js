
$(document).ready(function() {

    $('[name="birth_date"]').datepicker({
        format: 'dd.mm.yyyy',
        autoclose: true,
        forseParse: false,
        keyboardNavigation: false
    });

    $('[name="phone"]').inputmask("99999999999");

    $('#fileupload').fileupload({
        url: $('#img_div').data('url'),
        done: function (e, data) {
            $('#profile_img').attr('src', data.result['img_url']);
            $('#temp_file').val(data.result['img_url']);
        },
    });
});
