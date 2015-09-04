# coding: utf-8
from __future__ import unicode_literals

import uuid
import json

from django.contrib.auth import authenticate, login, logout
from django.utils.decorators import method_decorator
from django.contrib.auth.decorators import login_required
from django.shortcuts import render
from django.views.generic import TemplateView, View
from django.http import HttpResponseRedirect
from django.core.urlresolvers import reverse
from django.contrib.auth.models import check_password
from django.conf import settings
from django.core.mail import send_mail
from django.http import HttpResponse

from .models import User
from .helpers import Settings


class BaseView(View):

    def redirect(self, reverse_name, args=None, **kwargs):
        return HttpResponseRedirect(reverse(reverse_name, args=args), **kwargs)

    def redirect_to_url(self, url, **kwargs):
        return HttpResponseRedirect(url, **kwargs)

    def json_response(self, context, **response_kwargs):
        response_kwargs['content_type'] = 'application/json'
        return HttpResponse(json.dumps(context), **response_kwargs)


class BaseTemplateView(TemplateView):

    def redirect(self, reverse_name, args=None, **kwargs):
        return HttpResponseRedirect(reverse(reverse_name, args=args), **kwargs)

    def redirect_to_url(self, url, **kwargs):
        return HttpResponseRedirect(url, **kwargs)


class HomeView(BaseTemplateView):

    @method_decorator(login_required)
    def dispatch(self, *args, **kwargs):
        return super(HomeView, self).dispatch(*args, **kwargs)

    def get(self, request, *args, **kwargs):
        return render(request, "core/home.html")


class LoginView(BaseTemplateView):
    template_name = 'core/login.html'

    def get(self, request, *args, **kwargs):
        if request.user.is_authenticated():
            return self.redirect_to_url("/")
        return self.render_to_response({'regUrl': '/registration'})

    def post(self, request, *args, **kwargs):
        post = request.POST

        username = post['username']
        password=post['password']
        user = None

        # if email (костылим):
        if '@' in username:
            users = User.objects.filter(email=username)
            for us in users:
                if check_password(password, us.password):
                    user = authenticate(
                        username=us.username,
                        password=password
                    )
                    break
        else:
            user = authenticate(
                username=username,
                password=password
            )

        if user is not None:
            if user.is_active:
                login(request, user)
                return self.redirect("core:home")
            else:
                if not user.email:
                    return self.render_to_response({
                        'error': 'Пользователь не имеет почты!'
                    })

                host = Settings.get_host(request)
                code = uuid.uuid4().hex
                message = '{0}/set_user_active?uuid={1}'.format(host, code)

                user.verify_email_uuid = code
                user.save()

                send_mail('Подтверждение!', message, settings.EMAIL_HOST_USER,
                          [user.email], fail_silently=False)

                return self.render_to_response({
                    'error': 'Пользователь заблокирован! Проверьте свою почту!'
                })

        return self.render_to_response({
            'error': 'Неправильный логин или пароль!'
        })


class LogoutView(BaseView):

    def get(self, request, *args, **kwargs):
        logout(request)
        return self.redirect('login')


class RegistrationView(BaseView):

    def post(self, request, *args, **kwargs):

        post = request.POST

        host = Settings.get_host(request)
        code = uuid.uuid4().hex
        message = '{0}/set_user_active?uuid={0}'.format(host, code)

        user = User(
            username=post.get('login'),
            email=post.get('email'),
            is_active=False,
            is_staff=True,
            verify_email_uuid=code
        )
        user.set_password(post.get('password'))
        user.save()

        send_mail('Подтверждение!', message, settings.EMAIL_HOST_USER,
                  [user.email], fail_silently=False)

        return self.json_response(
            {'error': 'Регистрация прошла успешно! На почту была отправлена инструкция по активации аккаунта.'})


class SetUserActive(BaseView):

    # тут насувать мессаджей на логин page

    def get(self, request, *args, **kwargs):
        uuid4 = request.GET.get('uuid', None)
        if not uuid4:
            return self.redirect('login')

        try:
            user = User.objects.get(verify_email_uuid=uuid4)
        except User.DoesNotExist:
            return self.redirect('login')
        else:
            user.is_active = True
            user.verify_email_uuid = None
            user.save()

        user.backend = 'django.contrib.auth.backends.ModelBackend'

        login(request, user)

        return self.redirect('home')
