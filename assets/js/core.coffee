modalReg = null

$(document).ready( ->
  csrftoken = UserService.getCookie('csrftoken');
  modalReg = $('#modal-register');
);

class UserService
  @showRegistration: ->
    modalReg.modal('show');
    $("#registration-message").attr("class", "alert").html("");
  @register: (url) ->
    regForm = $('#regi_form');
    if(!regForm.valid())
        return false
    validator = regForm.data('validator')
    if($('[name="reg_password"]').val() != $('[name="reg_confirm"]').val())
        validator.showErrors({'error': 'Не совпадение паролей'})
        return false

    $.post(url, {
            csrfmiddlewaretoken: regForm.find('[name="csrfmiddlewaretoken"]').val(),
            email: $('[name="reg_email"]').val(),
            login: $('[name="reg_login"]').val(),
            password: $('[name="reg_password"]').val()
        }, (response) ->
            if (response['status'] == 'ok')
                $("#modal-register .modal-body").html("<div class='alert alert-info'>" + response['message'] + "</div>");
            else
                $('#registration-message').addClass("alert-error").text(response['message'])
    )

  @getCookie: (name) ->
    cookieValue = null
    if (document.cookie && document.cookie != '')
        cookies = document.cookie.split(';');

        for i in cookies.length
            cookie = jQuery.trim(cookies[i])
            # Does this cookie string begin with the name we want?
            if (cookie.substring(0, name.length + 1) == (name + '='))
                cookieValue = decodeURIComponent(cookie.substring(name.length + 1));
                break
    return cookieValue;