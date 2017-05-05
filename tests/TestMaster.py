from master import Master
from TestStorage import Test_T
from unittest import TestCase, main

class TestSingleton(TestCase):
    class __AnotherMasterClass:
        def __init__(self):
            self.mstr = Master(5)
        def get_m(self): return self.mstr

    def test_identity(self):
        m = Master()
        cls = TestSingleton.__AnotherMasterClass()
        k = cls.get_m()
        self.assertEqual(m.id(), k.id())
        self.assertNotEqual(id(m), id(k))

class TestMasterMethods(TestCase):
    def test_init(self):
        self.failUnlessRaises(ValueError, Master, 0)
        self.failUnlessRaises(ValueError, Master, -1)

    def test_get(self):
        m = Master()
        t = Test_T(5)
        t_id = id(t)
        m.send(t)
        t_ = m.get(t_id)
        self.assertEqual(t, t_)

        m.remove(t_id)
        self.assertRaises(KeyError, m.get, t_id)

    def test_send(self):
        m = Master()
        for x in range(5):
            m.send(Test_T(x))
        self.assertEqual(m.total_cnt(), 5)
        self.assertEqual(m.active_cnt(), 2)

    def test_remove(self): pass
    def test_pause(self): pass
    def test_resume(self): pass
    def test_queue(self): pass
    def test_status(self): pass
    def test_get_progress(self): pass
    def test_shutdown(self): pass
    def test_exit(self): pass
    def test_log_parse(self): pass
    def test_app_recovery(self): pass
    def test_init_save(self): pass
    def test_save_id(self): pass
    def test_save_page(self): pass
    def test_save_size(self): pass
    def test_app_crash(self): pass

if __name__ == '__main__':
    # Run all of the tests, print the results, and exit.
    main(verbosity=2)