var gulp = require('gulp'),
    concat = require('gulp-concat'),
    gls = require('gulp-live-server'),
    less = require('gulp-less'),
    paths = {
      src: 'src/**/*',
      static: 'dist',
      less: ['src/less/**/*.less', 'src/less/*.less'], distStyles: 'dist',
      templates: 'src/**/*.html', distTemplates: 'dist',
      scripts: 'src/**/*.js', distScript: 'dist',
      distStylesFilename: 'style.css', distScriptFilename: 'main.js',
      dist: 'dist/**/*'
    };

gulp.task('less', function () {
  return gulp
    .src(paths.less)
    .pipe(less())
    .pipe(concat(paths.distStylesFilename))
    .pipe(gulp.dest(paths.distStyles));
});

gulp.task('scripts', function() {
  return gulp
    .src(paths.scripts)
    .pipe(concat(paths.distScriptFilename))
    .pipe(gulp.dest(paths.distScript));
});

gulp.task('templates', function() {
  return gulp
    .src(paths.templates)
    .pipe(gulp.dest(paths.distTemplates));
});

gulp.task('build', ['less', 'templates', 'scripts']);

gulp.task('serve', function() {
  var server = gls.static(paths.static, 8888);
  server.start();

  gulp.watch([paths.dist], watch);
  function watch(file) {
    server.notify.apply(server, [file]);
  }
});

gulp.task('watch', function() {
  gulp.watch(paths.less, ['less']);
  gulp.watch([paths.scripts], ['scripts']);
  gulp.watch([paths.templates], ['templates']);
});

gulp.task('default',['build', 'serve', 'watch']);

