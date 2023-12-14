from tkinter import *
import os
#Baixe a biblioteca pytube - # pip install pytube
from pytube import YouTube
from pytube.cli import on_progress
#Programa para Baixar Videos e Audios do YouTube
#Funçao na qual baixa o video ou audio 
#------------------------------------------------------------
def baixar():
  link = formulario.get()
  url = YouTube(link, on_progress_callback= on_progress)
  os.chdir(os.path.join(os.path.expanduser('~'),'desktop'))

  status1 = f'''
  Baixado Com sucesso'''
  status["text"] = status1
  ys = url.streams.get_highest_resolution()
  ys.download()

#-------------------------------------------------------------
#Programa de Interface e Configuração
video = Tk()
video.title("Software@Luc")
video.geometry('800x400')
#--------------------------------------------------------------
#Dentro da Interface Gráfica
texto = Label(video, text="Baixe Videos do Youtube").pack(pady=30)

formulario = Entry(video)
formulario.pack(ipadx=60)

Button(video, text="Clique Aqui", command=baixar).pack(pady=10)

legen = Label(video, text="Status:")
legen.pack(pady=10)

status = Label(video, text="--------")
status.pack()
#----------------------------------------------------------------
#Programa Rodando
video.mainloop()
