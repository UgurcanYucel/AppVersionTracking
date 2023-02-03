from src.source.AppVersion import get_android_version, get_ios_version

from src.config import APP_DICT

if __name__ == '__main__':

    for app in APP_DICT:
        try:
            get_ios_version(app['app_name'], app['app_id'])
        except Exception as e:
            print('ios error log any where', e)
        try:
            get_android_version(app['android_app_id'])
        except Exception as e:
            print('android error log any where', e)
