#! usr/bin/env python
# -*- coding:utf-8 -*-

__author__ = "Peter Hu"

'''
ormģ����Ƶ�ԭ��
    1. �򻯲���
        sql������������ ��ϵ�����ݣ� ��python�������Ƕ���Ϊ�˼򻯱�� ������Ҫ�����ǽ���ӳ��
        ӳ���ϵΪ��
            �� ==>  ��
            �� ==> ʵ��
���orm�ӿڣ�
    1. ���ԭ��
        �����ϲ��������Ƽ����õ�API�ӿ�
    2. ��Ƶ��ýӿ�
        1. �� <==> ��
            ͨ��������� ��ӳ�������ԣ��������ֶ����� �ֶ����ԣ�
                from transwarp.orm import Model, StringField, IntegerField
                class User(Model):
                    __table__ = 'users'
                    id = IntegerField(primary_key=True)
                    name = StringField()
            ���п��Կ��� __table__ ӵ��ӳ������� id/name ����ӳ�� �ֶζ����ֶ��� �� �ֶ����ԣ�
        2. �� <==> ʵ��
            ͨ��ʵ�������� ��ӳ�� �е�ֵ
                # ����ʵ��:
                user = User(id=123, name='Michael')
                # �������ݿ�:
                user.insert()
            ��� id/name Ҫ��� userʵ��������
'''

import time, logging, db

class Field(object):
    '''
    �������ݿ��еı��  �ֶ�����
    _count: �����ԣ�ûʵ����һ�Σ���ֵ��+1
    self._order: ʵ�����ԣ� ʵ����ʱ�������Դ��õ������ڼ�¼ ��ʵ���� ����ĵڶ��ٸ�ʵ��
        ��������doctest��
            ����userʱ���������5��ʵ�������������ֶ�����
                id = IntegerField(primary_key=True)
                name = StringField()
                email = StringField(updatable=False)
                passwd = StringField(default=lambda: '******')
                last_modified = FloatField()
            ����ʵ����_order ���Ծ���������
                INFO:root:[TEST _COUNT] name => 1
                INFO:root:[TEST _COUNT] passwd => 3
                INFO:root:[TEST _COUNT] id => 0
                INFO:root:[TEST _COUNT] last_modified => 4
                INFO:root:[TEST _COUNT] email => 2
            �������__sqlʱ����_gen_sql ����������Щ�ֶξ��ǰ�������
                create table `user` (
                `id` bigint not null,
                `name` varchar(255) not null,
                `email` varchar(255) not null,
                `passwd` varchar(255) not null,
                `last_modified` real not null,
                primary key(`id`)
                );
    self._default: ������orm�Լ�����ȱʡֵ��ȱʡֵ������ �ɵ��ö��󣬱��纯��
                ���磺passwd �ֶ� <StringField:passwd,varchar(255),default(<function <lambda> at 0x0000000002A13898>),UI>
                     ����passwd��Ĭ��ֵ �Ϳ���ͨ�� ���صĺ��� ����ȡ��
    ������ʵ�����Զ������������ֶ����Ե�
    '''
    
    _count = 0
    
    def __init__(self, **kw):
        self.name = kw.get('name', None)
        self._default = kw.get('default', None)
        self.primary_key = kw.get('primary_key', False)
        self.nullable = kw.get('nullable', False)
        self.updatable = kw.get('updatable', True)
        self.insertable = kw.get('insertable', True)
        self.ddl = kw.get('ddl', '')
        self._order = Field._count
        Field._count = Field._count + 1
        
    @property
    def default(self):
        d = self._default
        return d() if callable(d) else d
        
    def __str__(self):
        s = ['<%s:%s,%s,default(%s),' % (self.__class__.__name__, self.name, self.ddl, self._default)]
        self.nullable and s.append('N')
        self.updatable and s.append('U')
        self.insertable and s.append('I')
        s.append('>')
        return ''.join(s)
        
class StringField(Field):
    
    def __init__(self, **kw):
        if not 'default' in kw:
            kw['default'] = ''
        if not 'ddl' in kw:
            kw['ddl'] = 'varchar(255)'
        super(StringField, self).__init__(**kw)

class IntegerField(Field):
    
    def __init__(self, **kw):
        if not 'default' in kw:
            kw['default'] = 0
        if not 'ddl' in kw:
            kw['ddl'] = 'bigint'
        super(IntegerField, self).__init__(**kw)

class FloatField(Field):
    
    def __init__(self, **kw):
        if not 'default' in kw:
            kw['default'] = 0.0
        if not 'ddl' in kw:
            kw['ddl'] = 'real'
        super(FloatField, self).__init__(**kw)

class BooleanField(Field):
    
    def __init__(self, **kw):
        if not 'default' in kw:
            kw['default'] = False
        if not 'ddl' in kw:
            kw['ddl'] = 'bool'
        super(BooleanField, self).__init__(**kw)

class TextField(Field):
    
    def __init__(self, **kw):
        if not 'default' in kw:
            kw['default'] = ''
        if not 'ddl' in kw:
            kw['ddl'] = 'text'
        super(TextField, self).__init__(**kw)

class BolbField(Field):
    
    def __init__(self, **kw):
        if not 'default' in kw:
            kw['default'] = ''
        if not 'ddl' in kw:
            kw['ddl'] = 'blob'
        super(BolbField, self).__init__(**kw)

class VersionField(Field):
    
    def __init__(self, name=None):
        super(VersionField, self).__init__(name=name, default=0, ddl='bigint')

_triggers = frozenset(['pre_insert', 'pre_update', 'pre_delete'])

def _gen_sql(table_name, mappings):
    '''
    �� ==> ��ʱ ���ɴ������sql
    '''
    pk = None
    sql = ['-- generating SQL for %s:' % table_name, 'create table `%s` (' % table_name]
    for f in sorted(mappings.values(), lambda x, y: cmp(x._order, y._order)):
        if not hasattr(f, 'ddl'):
            raise StandardError('no ddl in field "%s".' % f)
        ddl = f.ddl
        nullable = f.nullable
        if f.primary_key:
            pk = f.name
        sql.append(nullable and '  `%s` %s,' % ( f.name, ddl) or '  `%s` %s not null,' % (f.name, ddl))
    sql.append('  primary key(`%s`)' % pk)
    sql.append(');')
    return '\n'.join(sql)
        
class ModelMetaclass(type):
    '''
    �������̬������²���
    �����޸�Model�ࣺ
        1. �ų���Model����޸�
    �������ֶε�mapping��
        1. ����������ֵ�����ȡ�� �����Ժ��ֶ��� ��mapping
        2. ��ȡ��ɺ��Ƴ���Щ�����ԣ������ʵ�����Գ�ͻ
        3. ����"__mappings__" ���ԣ�������ȡ������mapping����
    ��ͱ��mapping��
        1. ��ȡ����������Ϊ��������ɼ򵥵���ͱ��ӳ��
        2. ����"__table__"���ԣ�������ȡ�����ı���
    '''
    def __new__(cls, name, bases, attrs):
        # skip base Model class:
        if name=='Model':
            return type.__new__(cls, name, bases, attrs)
        
        # store all subclasses info:
        if not hasattr(cls, 'subclasses'):
            cls.subclasses = {}
        if not name in cls.subclasses:
            cls.subclasses[name] = name
        else:
            logging.warning('Redefine class: %s' % name)
            
        logging.info('Scan ORMapping %s...' % name)
        mappings = dict()
        primary_key = None
        for k, v in attrs.iteritems():
            if isinstance(v, Field):
                if not v.name:
                    v.name = k
                logging.info('Found mapping: %s => %s' % (k, v))
                # check duplicate primary key:
                if v.primary_key:
                    if primary_key:
                        raise TypeError('Cannot define more than 1 primary key in class: %s' % name)
                    if v.updatable:
                        logging.warning('NOTE: change primary key to non-updatable')
                        v.updatable = False
                    if v.nullable:
                        logging.warning('NOTE: change primary key to non-nullable.')
                        v.nullable = False
                    primary_key = v
                mappings[k] = v
        # check exist of primary key:
        if not primary_key:
            raise TypeError('Primary key not defined in class: %s' % name)
        for k in mappings.iterkeys():
            attrs.pop(k)
        if not '__table__' in attrs:
            attrs['__table__'] = name.lower()
        attrs['__mappings__'] = mappings
        attrs['__primary_key__'] = primary_key
        attrs["__sql__"] = lambda self: _gen_sql(attrs['__table__'], mappings)
        for trigger in _triggers:
            if not trigger in attrs:
                attrs[trigger] = None
        return type.__new__(cls, name, bases, attrs)
        
class Model(dict):
    '''
    ����һ�����࣬�û��������� ����ӳ���ϵ�� ���������Ҫ��̬ɨ���������� ��
    ���г�ȡ�������ԣ� ��� �� <==> �� ��ӳ�䣬 ����ʹ�� metaclass ��ʵ�֡�
    ���ɨ������Ľ�������ڳ�������
        "__table__" : ����
        "__mappings__": �ֶζ���(�ֶε��������ԣ���Field��)
        "__primary_key__": �����ֶ�
        "__sql__": ������ʱִ�е�sql
        
    ������ʵ����ʱ����Ҫ��� ʵ������ <==> ��ֵ ��ӳ�䣬 ����ʹ�� ����dict ��ʵ�֡�
        Model ���ֵ�̳ж���������ͨ��"__getattr__","__setattr__"��Model��д��
        ʹ������javascript�е� object��������������ͨ�����Է��� ֵ���� a.key = value
        
    >>> class User(Model):
    ...     id = IntegerField(primary_key=True)
    ...     name = StringField()
    ...     email = StringField(updatable=False)
    ...     passwd = StringField(default=lambda: '******')
    ...     last_modified = FloatField()
    ...     def pre_insert(self):
    ...         self.last_modified = time.time()
    >>> u = User(id=10190, name='Michael', email='orm@db.org')
    >>> r = u.insert()
    >>> u.email
    'orm@db.org'
    >>> u.passwd
    '******'
    >>> u.last_modified > (time.time() - 2)
    True
    >>> f = User.get(10190)
    >>> f.name
    u'Michael'
    >>> f.email
    u'orm@db.org'
    >>> f.email = 'changed@db.org'
    >>> r = f.update() # change email but email is non-updatable!
    >>> len(User.find_all())
    1
    >>> g = User.get(10190)
    >>> g.email
    u'orm@db.org'
    >>> r = g.delete()
    >>> len(db.select('select * from user where id=10190'))
    0
    >>> import json
    >>> print User().__sql__()
    -- generating SQL for user:
    create table `user` (
      `id` bigint not null,
      `name` varchar(255) not null,
      `email` varchar(255) not null,
      `passwd` varchar(255) not null,
      `last_modified` real not null,
      primary key(`id`)
    );
    '''
    __metaclass__ = ModelMetaclass
    
    def __init__(self, **kw):
        super(Model, self).__init__(**kw)
        
    def __getattr__(self, key):
        try:
            return self[key]
        except KeyError:
            raise AttributeError(r"'Dict' object has no attribute '%s'" % key)

    def __setattr__(self, key, value):
        self[key] = value
    
    @classmethod
    def get(cls, pk):
        '''
        Get by primary key.
        '''
        d = db.select_one('select * from %s where %s=?' % (cls.__table__, cls.__primary_key__.name), pk)
        return cls(**d) if d else None
    
    @classmethod
    def find_first(cls, where, *args):
        '''
        ͨ��where������������ѯ������1����ѯ���������ж����ѯ���
        ��ȡ��һ�������û�н�����򷵻�None
        '''
        d = db.select_one('select * from %s %s' % (cls.__table__, where), *args) 
        return cls(**d) if d else None
    
    @classmethod
    def find_all(cls, *args):
        '''
        ��ѯ�����ֶΣ� �������һ���б���
        '''
        L = db.select('select * from `%s`' % cls.__table__)
        return [cls(**d) for d in L]
    
    @classmethod
    def find_by(cls, where, *args):
        '''
        ͨ��where������������ѯ���������һ���б���
        '''
        L = db.select('select * from `%s` %s' % (cls.__table__, where), *args)
        return [cls(**d) for d in L]
    
    @classmethod
    def count_all(cls):
        '''
        ִ�� select count(pk) from table��䣬����һ����ֵ
        '''
        return db.select_int('select count(`%s`) from `%s`' % (cls.__primary_key__.name, cls.__table__))
    
    @classmethod
    def count_by(cls, where, *args):
        '''
        ͨ��select count(pk) from table where ...�����в�ѯ�� ����һ����ֵ
        '''
        return db.select_int('select count(`%s`) from `%s` %s' % (cls.__primary_key__.name, cls.__table__, where), *args)
    
    def update(self):
        '''
        ������е��ֶ������� updatable��������ֶο��Ա�����
        ���ڶ���ı��̳�Model���ࣩ��һ�� Dict���󣬼�ֵ����ʵ��������
        ���Կ���ͨ���������ж� �û��Ƿ����˸��ֶε�ֵ
            ��������ԣ� ��ʹ���û������ֵ
            ��������ԣ� ������ֶζ���� default���Դ���
            ����� Field�� �� default ����
        ͨ����db�����update�ӿ�ִ��SQL
            SQL: update `user` set `passwd`=%s,`last_modified`=%s,`name`=%s where id=%s,
                 ARGS: (u'******', 1441878476.202391, u'Michael', 10190
        '''
        self.pre_update and self.pre_update()
        L = []
        args = []
        for k, v in self.__mappings__.iteritems():
            if v.updatable:
                if hasattr(self, k):
                    arg = getattr(self, k)
                else:
                    arg = v.default
                    setattr(self, k, arg)
                L.append('`%s`=?' % k)
                args.append(arg)
        pk = self.__primary_key__.name
        args.append(getattr(self, pk))
        db.update('update `%s` set %s where %s=?' % (self.__table__, ','.join(L), pk), *args)
        return self

    def delete(self):
        '''
        ͨ��db����� update�ӿ� ִ��SQL
            SQL: delete from `user` where `id`=%s, ARGS: (10190,)
        '''
        self.pre_delete and self.pre_delete()
        pk = self.__primary_key__.name
        args = (getattr(self, pk),)
        db.update('delete from `%s` where `%s`=?' % (self.__table__, pk), *args)
        return self
    
    def insert(self):
        '''
        ͨ��db�����insert�ӿ�ִ��SQL
            SQL: insert into `user` (`passwd`,`last_modified`,`id`,`name`,`email`) values (%s,%s,%s,%s,%s),
            ���������� ARGS: ('******', 1441878476.202391, 10190, 'Michael', 'orm@db.org')
        '''
        self.pre_insert and self.pre_insert()
        params = {}
        for k, v in self.__mappings__.iteritems():
            if v.insertable:
                if not hasattr(self, k):
                    setattr(self, k, v.default)
                params[v.name] = getattr(self, k)
        db.insert('%s' % self.__table__, **params)
        return self
        
if __name__=='__main__':
    logging.basicConfig(level=logging.DEBUG)
    db.create_engine('root', 'password', 'test')
    db.update('drop table if exists user')
    db.update('create table user (id int primary key, name text, email text, passwd text, last_modified real)')
    import doctest
    doctest.testmod()