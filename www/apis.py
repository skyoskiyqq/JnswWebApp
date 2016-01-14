#!/usr/bin/env python
# -*- coding: utf-8 -*-

__author__ = 'Peter Hu'

"""
ʵ����Json���ݸ�ʽ���н�����RESTful API
���ԭ��
    ����API���ǰ�Web App�Ĺ���ȫ����װ�ˣ����ԣ�ͨ��API�������ݣ�
    ���Լ���ذ�ǰ�˺ͺ�˵Ĵ�����룬ʹ�ú�˴������ڲ��ԣ�
    ǰ�˴����д����
ʵ�ַ�ʽ��
    һ��APIҲ��һ��URL�Ĵ�����������ϣ����ֱ��ͨ��һ��@api��
    �Ѻ������JSON��ʽ��REST API�� ���������Ҫʵ��һ��װ������
    �ɸ�װ������ �������ص����� ����� json ��ʽ
"""

import json, functools, logging
from transwarp.web import ctx

def dumps(obj):
    """
    Serialize ``obj`` to a JSON formatted ``str``.
    ���л�����
    """
    return json.dumps(obj)
    
class APIError(StandardError):
    """
    the base APIError which contains error(required), data(optional) and message(optional).
    �洢����API �쳣���������
    """
    def __init__(self, error, data='', message=''):
        super(APIError, self).__init__(message)
        self.error = error
        self.data = data
        self.message = message

class APIValueError(APIError):
    """
    Indicate the input value has error or invalid. The data specifies the error field of input form.
    ���벻�Ϸ� �쳣����
    """
    def __init__(self, field, message=''):
        super(APIValueError, self).__init__('value:invalid', field, message)


class APIResourceNotFoundError(APIError):
    """
    Indicate the resource was not found. The data specifies the resource name.
    ��Դδ�ҵ� �쳣����
    """
    def __init__(self, field, message=''):
        super(APIResourceNotFoundError, self).__init__('value:notfound', field, message)


class APIPermissionError(APIError):
    """
    Indicate the api has no permission.
    Ȩ�� �쳣����
    """
    def __init__(self, message=''):
        super(APIPermissionError, self).__init__('permission:forbidden', 'permission', message)

def api(func):
    """
    A decorator that makes a function to json api, makes the return value as json.
    ���������ؽ�� ת����json ��װ����
    @api��Ҫ��Error���д������Ƕ���һ��APIError��
    ����Error��ָAPI����ʱ�������߼����󣨱����û������ڣ�
    ������Error��ΪBug�����صĴ������Ϊinternalerror
    @app.route('/api/test')
    @api
    def api_test():
        return dict(result='123', items=[])
    """
    @functools.wraps(func)
    def _wrapper(*args, **kw):
        try:
            r = dumps(func(*args, **kw))
        except APIError, e:
            r = json.dumps(dict(error=e.error, data=e.data, message=e.message))
        except Exception, e:
            logging.exception(e)
            r = json.dumps(dict(error='internalerror', data=e.__class__.__name__, message=e.message))
        ctx.response.content_type = 'application/json'
        return r
    return _wrapper

if __name__=='__main__':
    import doctest
    doctest.testmod()