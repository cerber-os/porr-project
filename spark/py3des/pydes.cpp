#include <stdint.h>
#include <Python.h>

#include "des3.h"

typedef DES3* pyDES;
DES3 gDES(0, 0, 0, 0);

extern "C" {
    PyObject* createPyDES(PyObject *self, PyObject *args);
    PyObject* encryptBlock(PyObject *self, PyObject *args);
    uint64_t decryptBlock(uint64_t block);
    void destroyPyDES(PyObject *);
}

PyObject* createPyDES(PyObject *self, PyObject *args) {
    uint64_t key1, key2, key3;
    if(!PyArg_ParseTuple(args, "KKK", &key1, &key2, &key3))
        return NULL;
    
    pyDES des = new DES3(0, key1, key2, key3);
    return PyCapsule_New(des, "3DES instance", destroyPyDES);
}

PyObject* encryptBlock(PyObject *self, PyObject *args) {
    uint64_t block, result;
    PyObject* capsule;
    
    if(!PyArg_ParseTuple(args, "OK", &capsule, &block))
        return NULL;

    //pyDES des = (pyDES) PyCapsule_GetPointer(capsule, "3DES instance");
    result = gDES.encrypt(block);
    return Py_BuildValue("K", result);
}

PyObject* decryptBlock(PyObject *self, PyObject *args) {
    uint64_t block, result;
    PyObject* capsule;
    
    if(!PyArg_ParseTuple(args, "OK", &capsule, &block))
        return NULL;

    //pyDES des = (pyDES) PyCapsule_GetPointer(capsule, "3DES instance");
    result = gDES.decrypt(block);
    return Py_BuildValue("K", result);
}

void destroyPyDES(PyObject * obj) {
    delete (pyDES) PyCapsule_GetPointer(obj, "3DES instance");
}





char createPyDES_docs[] = "Create 3DES instance";
char encryptBlock_docs[] = "Encrypt a single block using 3DES algorithm";
char decryptBlock_docs[] = "Decrypt a single block using 3DES algorithm";

PyMethodDef pydes_funcs[] = {
	{	"createPyDES",
		(PyCFunction)createPyDES,
		METH_VARARGS,
		createPyDES_docs},
	{	"encryptBlock",
		(PyCFunction)encryptBlock,
		METH_VARARGS,
		encryptBlock_docs},
    {	"decryptBlock",
		(PyCFunction)decryptBlock,
		METH_VARARGS,
		decryptBlock_docs},

	{	NULL}
};

char pydes_docs[] = "3DES implementation in C for Python";

PyModuleDef pydes_mod = {
	PyModuleDef_HEAD_INIT,
	"libpydes",
	pydes_docs,
	-1,
	pydes_funcs,
	NULL,
	NULL,
	NULL,
	NULL
};

PyMODINIT_FUNC PyInit_libpydes(void) {
	return PyModule_Create(&pydes_mod);
}
