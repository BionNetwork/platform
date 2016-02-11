var gulp = require('gulp'),
    concat = require('gulp-concat'),
    gls = require('gulp-live-server'),
    paths = {
      src: 'src/**/*',
      static: 'dist',
      templates: 'src/**/*.html', distTemplates: 'dist',
      scripts: 'src/**/*.js', distScriptPath: 'dist', distScriptFilename: 'main.js',
      dist: 'dist/**/*'
    };

gulp.task('scripts', function() {
  return gulp
    .src([paths.scripts])
    .pipe(concat(paths.distScriptFilename))
    .pipe(gulp.dest(paths.distScriptPath));
});

gulp.task('templates', function() {
  return gulp
    .src([paths.templates])
    .pipe(gulp.dest(paths.distTemplates));
});

gulp.task('build', ['templates', 'scripts']);

gulp.task('serve', function() {
  var server = gls.static(paths.static, 8888);
  server.start();

  gulp.watch([paths.dist], watch);
  function watch(file) {
    server.notify.apply(server, [file]);
  }
});

gulp.task('watch', function() {
  gulp.watch([paths.scripts], ['scripts']);
  gulp.watch([paths.templates], ['templates']);
});

gulp.task('default',['build', 'serve', 'watch']);

