# coding: utf-8
from __future__ import unicode_literals

import uuid
import json
import logging
import os
from PIL import Image
import StringIO

from django.core.files.uploadedfile import InMemoryUploadedFile
from django.core.files import File
from django.contrib.auth import authenticate, login, logout
from django.utils.decorators import method_decorator
from django.contrib.auth.decorators import login_required
from django.shortcuts import render, get_object_or_404, redirect
from django.views.generic import TemplateView, View
from django.http import HttpResponseRedirect
from django.core.urlresolvers import reverse
from django.contrib.auth.models import check_password
from django.conf import settings
from django.core.mail import send_mail
from django.http import HttpResponse
from django.db.models import Q
from django.core.paginator import Paginator

from smtplib import SMTPServerDisconnected
from .models import User
from .helpers import Settings, CustomJsonEncoder
from . import forms as core_forms

logger = logging.getLogger(__name__)


class BaseView(View):

    def redirect(self, reverse_name, args=None, **kwargs):
        return HttpResponseRedirect(reverse(reverse_name, args=args), **kwargs)

    def redirect_to_url(self, url, **kwargs):
        return HttpResponseRedirect(url, **kwargs)

    def json_response(self, context, **response_kwargs):
        response_kwargs['content_type'] = 'application/json'
        return HttpResponse(
            json.dumps(context, cls=CustomJsonEncoder), **response_kwargs)


class BaseTemplateView(TemplateView):

    def redirect(self, reverse_name, args=None, **kwargs):
        return HttpResponseRedirect(reverse(reverse_name, args=args), **kwargs)

    def redirect_to_url(self, url, **kwargs):
        return HttpResponseRedirect(url, **kwargs)

    def json_response(self, context, **response_kwargs):
        response_kwargs['content_type'] = 'application/json'
        return HttpResponse(
            json.dumps(context, cls=CustomJsonEncoder), **response_kwargs)


class HomeView(BaseTemplateView):
    """Главная страница dashboard"""
    @method_decorator(login_required)
    def dispatch(self, *args, **kwargs):
        return super(HomeView, self).dispatch(*args, **kwargs)

    def get(self, request, *args, **kwargs):
        return render(request, "core/home.html")


class LoginView(BaseTemplateView):
    """
    Вход пользователя в систему
    """
    template_name = 'core/login.html'

    def get(self, request, *args, **kwargs):
        if request.user.is_authenticated():
            return self.redirect_to_url("/")
        return self.render_to_response({'regUrl': '/registration'})

    def post(self, request, *args, **kwargs):
        post = request.POST

        username = post['username']
        password = post['password']
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
                message = '{0}{2}?uuid={1}'.format(host, code, reverse('core:activate_user'))

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
    """
    Выход пользователя
    """
    def get(self, request, *args, **kwargs):
        logout(request)
        return self.redirect('login')


class RegistrationView(BaseView):
    """
    Регистрация нового пользователя в системе
    """
    def post(self, request, *args, **kwargs):

        post = request.POST

        host = Settings.get_host(request)
        code = uuid.uuid4().hex
        message = '{0}{2}?uuid={1}'.format(host, code, reverse('core:activate_user'))

        user_login = post.get('login', '')
        user_email = post.get('email', '')

        try:
            if len(user_login) == 0 or len(user_email) == 0:
                raise ValueError("Логин или пароль не могут быть пустыми")

            current_user = User.objects.filter(Q(username=user_login) | Q(email=user_email))

            if len(current_user) > 0:
                raise ValueError("Такой пользователь уже существует")
            user = User(
                username=user_login,
                email=user_email,
                is_active=False,
                is_staff=True,
                verify_email_uuid=code
            )
            user.set_password(post.get('password'))
            user.save()

            # тут сделать очередь на письма
            send_mail('Подтверждение регистрации!', message, settings.EMAIL_HOST_USER,
                      [user.email], fail_silently=False)

            return self.json_response(
                {'status': 'ok',
                 'message': 'Регистрация прошла успешно! На почту была отправлена инструкция по активации аккаунта.'})
        except ValueError as e:
            return self.json_response(
                {'status': 'error', 'message': e.message}
            )
        except SMTPServerDisconnected as e:
            logger.exception(e.message)
            return self.json_response(
                {'status': 'error', 'message': "Ошибка при отправке почты %s" % e.message}
            )
        except:
            logger.exception("Произошла системная ошибка")
            return self.json_response(
                {'status': 'error', 'message': 'Произошла системная ошибка. Мы уже работаем над ней'}
            )


class SetUserActive(BaseView):
    """
    Активация пользователей
    """
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


class UserListView(BaseTemplateView):
    """
    Список пользователей в системе
    """
    template_name = 'core/users/index.html'

    def get(self, request, *args, **kwargs):

        get = request.GET
        or_cond = Q()

        search = get.get('search', None)
        if search:
            for field in ('username', 'email', 'id'):
                or_cond |= Q(
                    **{"%s__icontains" % field: search}
                )
        users = User.objects.filter(or_cond)
        count = 20

        paginator = Paginator(users, count)
        page_count = paginator.num_pages

        page = int(get.get('page', 0))
        if page not in xrange(page_count):
            page = 0

        users = paginator.page(page + 1)

        return self.render_to_response(
            {
                'users': users,
                'range': range(page_count),
                'page': page,
                'max': page_count-1,
                'search': search or ''
            }
        )


class RemoveUserView(BaseView):
    """
    Удаление пользователя
    """
    def post(self, request, *args, **kwargs):
        user = get_object_or_404(User, pk=kwargs.get('id'))
        user.delete()

        return self.json_response({'redirect_url': reverse('core:users')})


class NewUserView(BaseTemplateView):
    """
    Создание нового пользователя
    """
    template_name = 'core/users/add.html'

    def get(self, request, *args, **kwargs):
        form = core_forms.NewUserForm()
        return render(request, self.template_name, {'form': form})

    def post(self, request, *args, **kwargs):
        post = request.POST
        form = core_forms.NewUserForm(post)

        if not form.is_valid():
            print form.errors
            return self.render_to_response({'form': form})

        user = form.save(commit=False)
        user.is_active = False
        user.is_staff = True
        user.set_password(post.get('password'))
        user.save()

        return self.redirect('core:users')


class EditUserView(BaseTemplateView):
    """
    Редактирование пользователя
    """
    template_name = 'core/users/edit.html'

    def get(self, request, *args, **kwargs):

        user = get_object_or_404(User, pk=kwargs.get('id'))

        form = core_forms.UserForm(instance=user)
        return self.render_to_response({
            'form': form
        })

    def post(self, request, *args, **kwargs):

        post = request.POST
        user = get_object_or_404(User, pk=kwargs.get('id'))
        form = core_forms.UserForm(post, instance=user)

        if not form.is_valid():
            return self.render_to_response({'form': form})

        form.save()

        return self.redirect('core:users')


class UserProfileView(BaseTemplateView):
    """
    Страница профиля пользователя
    """
    template_name = 'core/users/profile.html'

    def get(self, request, *args, **kwargs):
        user = get_object_or_404(User, pk=kwargs.get('id'))

        form = core_forms.UserProfileForm(instance=user)
        return render(request, self.template_name, {'form': form, })

    @staticmethod
    def save_file_remove(fpath):
        if os.path.exists(fpath):
            os.remove(fpath)

    def post(self, request, *args, **kwargs):

        post = request.POST
        user_id = kwargs.get('id')
        user = get_object_or_404(User, pk=user_id)
        form = core_forms.UserProfileForm(post, instance=user)

        if not form.is_valid():
            return self.render_to_response({'form': form})

        user = form.save(commit=False)
        user.is_active = True

        old_big_file = user.avatar
        old_small_file = user.avatar_small

        # work with photos
        temp_file = post.get('temp_file')

        if not old_big_file:
            if temp_file:
                file_name = temp_file.rsplit(os.sep, 1)[-1]
                temp_dir = '{0}{1}'.format(settings.BASE_DIR, temp_file)

                avatar_img = Image.open(temp_dir)
                avatar_small_img = Image.open(temp_dir)

                big_img_io = StringIO.StringIO()
                small_img_io = StringIO.StringIO()

                avatar_img.save(big_img_io, format='JPEG')
                avatar_small_img.save(small_img_io, format='JPEG')

                avatar = InMemoryUploadedFile(
                    big_img_io, None, file_name, 'image/jpeg', big_img_io.len, None)

                avatar_small = InMemoryUploadedFile(
                    small_img_io, None, 'sm-{0}'.format(file_name),
                    'image/jpeg', small_img_io.len, None)

                user.avatar.save(file_name, avatar)
                user.avatar_small.save('sm-{0}'.format(file_name), avatar_small)

                # удаляем временный файл
                self.save_file_remove(temp_dir)
        else:
            if temp_file:
                file_name = temp_file.rsplit(os.sep, 1)[-1]
                old_file_name = old_big_file.name.rsplit(os.sep, 1)[-1]
                temp_dir = '{0}{1}'.format(settings.BASE_DIR, temp_file)

                if file_name != old_file_name:

                    old_big_file_path = old_big_file.path
                    old_small_file_path = old_small_file.path

                    avatar_img = Image.open(temp_dir)
                    avatar_small_img = Image.open(temp_dir)

                    big_img_io = StringIO.StringIO()
                    small_img_io = StringIO.StringIO()

                    avatar_img.save(big_img_io, format='JPEG')
                    avatar_small_img.save(small_img_io, format='JPEG')

                    avatar = InMemoryUploadedFile(
                        big_img_io, None, file_name, 'image/jpeg', big_img_io.len, None)

                    avatar_small = InMemoryUploadedFile(
                        small_img_io, None, 'sm-{0}'.format(file_name),
                        'image/jpeg', small_img_io.len, None)

                    user.avatar.save(file_name, avatar)
                    user.avatar_small.save('sm-{0}'.format(file_name), avatar_small)

                    # удаляем временный файл
                    self.save_file_remove(temp_dir)
                    # удаляем старое большое фото
                    # os.remove(old_file.path)
                    self.save_file_remove(old_big_file_path)
                    # удаляем старое мини фото
                    # os.remove(old_small_file.path)
                    self.save_file_remove(old_small_file_path)
            else:
                user.avatar = None
                self.save_file_remove(old_big_file.path)
                user.avatar_small = None
                self.save_file_remove(old_small_file.path)

        user.save()

        return redirect(reverse('users.profile', kwargs={'id': user_id}))


class TempImageView(BaseView):

    def post(self, request, *args, **kwargs):
        file_ = request.FILES['file']
        format_ = file_.name.rsplit('.', 1)[-1]
        name = '{0}.{1}'.format(str(uuid.uuid4()), format_)

        temp_dir = os.path.join(settings.MEDIA_ROOT, 'temporary')
        if not os.path.exists(temp_dir):
            os.makedirs(temp_dir)

        file_dir = os.path.join(temp_dir, name)

        with open(file_dir, 'wb+') as f:
            for chunk in file_.chunks():
                f.write(chunk)

        return self.json_response({
            'img_url': os.path.join(os.sep, 'media', 'temporary', name)
        })
