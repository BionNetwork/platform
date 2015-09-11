// Generated by CoffeeScript 1.9.3
var UserService, csrftoken, modalReg;

modalReg = null;

$(document).ready(function() {
  csrftoken = UserService.getCookie('csrftoken');
  return modalReg = $('#modal-register');
});

UserService = (function() {
  function UserService() {}

  UserService.showRegistration = function() {
    modalReg.modal('show');
    return $("#registration-message").attr("class", "alert").html("");
  };

  UserService.register = function(url) {
    var regForm, validator;
    regForm = $('#regi_form');
    if (!regForm.valid()) {
      return false;
    }
    validator = regForm.data('validator');
    if ($('[name="reg_password"]').val() !== $('[name="reg_confirm"]').val()) {
      $('#registration-message').addClass("alert-error").text('Не совпадение паролей');
      return false;
    }
    return $.post(url, {
      csrfmiddlewaretoken: csrftoken,
      email: $('[name="reg_email"]').val(),
      login: $('[name="reg_login"]').val(),
      password: $('[name="reg_password"]').val()
    }, function(response) {
      if (response['status'] === 'ok') {
        return $("#modal-register .modal-body").html("<div class='alert alert-info'>" + response['message'] + "</div>");
      } else {
        return $('#registration-message').addClass("alert-error").text(response['message']);
      }
    });
  };

  UserService.getCookie = function(name) {
    var cookie, cookieValue, cookies, i, j, len, ref;
    cookieValue = null;
    if (document.cookie && document.cookie !== '') {
      cookies = document.cookie.split(';');
      ref = cookies.length;
      for (j = 0, len = ref.length; j < len; j++) {
        i = ref[j];
        cookie = jQuery.trim(cookies[i]);
        if (cookie.substring(0, name.length + 1) === (name + '=')) {
          cookieValue = decodeURIComponent(cookie.substring(name.length + 1));
          break;
        }
      }
    }
    return cookieValue;
  };

  return UserService;

})();

//# sourceMappingURL=core.js.map
