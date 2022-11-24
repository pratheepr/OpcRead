# Create an exe of this code in Python 3.7 as a 32 bit application. Use Pyinstaller to generate the exe. The exe
# was created in Seenu's system. Doesnt work in Pratheep's system yet.
import pywintypes
import OpenOPC
import sys

#pywintypes.datetime = pywintypes.TimeType


def ConnectOPC(OPC_Server_Name):
    try:
        OPC_Client = OpenOPC.client()
        OPC_Conn_Obj = OPC_Client.connect(OPC_Server_Name)
        ret_value = 0
    except Exception as e:
        print('Exception in OPC Connect' + str(e))
        ret_value: Exception = e
    finally:
        return OPC_Client, ret_value
    # opc.connect('AIM.OPC.1')


def WriteToOPC(OPC_Conn_Obj, OPC_Address, Yax_Ewma):
    try:
        ret_value = OPC_Conn_Obj.write([(OPC_Address, Yax_Ewma)], include_error=True)
        # opc.write(('44AW04/OPC_TEST_CM:TEST_OPC_AI1.MEAS', 17.0))
        print(ret_value)
    except Exception as e:
        print('Exception in OPC Write' + str(e))
        ret_value = 'Fail'
    finally:
        return ret_value


if __name__ == "__main__":
    arguments = len(sys.argv) - 1

    OPC_Server_Name = sys.argv[1]
    OPC_Address = sys.argv[2]
    Yax_Ewma = sys.argv[3]
    Read_Wrt_Flg = sys.argv[4]

    OPC_Conn_Obj, ret = ConnectOPC(OPC_Server_Name)
    print("The script is called with arguments" + str(arguments))
    print(f'{OPC_Server_Name} {OPC_Address} {Yax_Ewma} {Read_Wrt_Flg}')

    pywintypes.datetime = pywintypes.TimeType

    OPC_Conn_Obj, ret = ConnectOPC(OPC_Server_Name)
    if Read_Wrt_Flg == 'W':
        ret = WriteToOPC(OPC_Conn_Obj, OPC_Address, Yax_Ewma)
    elif Read_Wrt_Flg == 'R':
        read_ret = OPC_Conn_Obj.read(OPC_Address)
        print(read_ret)


    #opc = OpenOPC.client()
    # opc.connect('AIM.OPC.1')
    #opc.connect('Matrikon.OPC.Simulation.1')
    #print(opc.read('Random.Int4'))
    # opc.write(('44AW04/OPC_TEST_CM:TEST_OPC_AI1.MEAS', 17.0))
    # opc.write(('Write Only.Int2', 109))
    #print(OPC_Conn_Obj.read('Write Only.Int2'))
