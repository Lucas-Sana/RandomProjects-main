import datetime
from tkinter import *
import tkinter
from datetime import datetime
#Configuração da Tela
janela = Tk()
janela.title("")
janela.geometry("440x180")
janela.resizable(width=False, height=False)
janela.config(bg="#3d3d3d")
cor1 ="#3d3d3d"
cor2 ="#fafcff"
#------------------------------
# Função relogio e datetime
def relogio():
    tempo = datetime.now()
    hora = tempo.strftime("%H:%M:%S")
    dia_semana = tempo.strftime("%A")
    dia = tempo.day
    mes = tempo.strftime("%B")
    ano = tempo.strftime("%Y")
    l1.config(text=hora)
    l1.after(200, relogio) # Deixa a hora diãmica
    l2.config(text=dia_semana + " " + str(dia) + " / " + str(mes) + " / " + str(ano))
#--------------------------------------
# Configuração do Label do tkinter  
l1=Label(janela, text="", font=("ROMAN 80"), bg=cor1, fg=cor2)
l1.grid(row=0, column=0, sticky=NW, padx=5)

l2=Label(janela, text="", font=("ITALIC 20"), bg=cor1, fg=cor2)
l2.grid(row=1, column=0, sticky=NW, padx=5)
#-----------------------------------------
relogio()
janela.mainloop()
 
