#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""رصد الأقصر - تطبيق محلي يجمع أخبار الأقصر من مئات المصادر وبعشر لغات.

التشغيل:      python3 luxor_news_app.py
ثم افتح:      http://localhost:8000
خيارات:       PORT=9000 python3 luxor_news_app.py     (منفذ آخر)
              python3 luxor_news_app.py --once        (جمع مرة واحدة وحفظ luxor_news.json و luxor_news.csv)
لا يحتاج أي مكتبات خارجية، ويتطلب اتصالا بالإنترنت وبايثون 3.8 أو أحدث.
"""
import csv, hashlib, html as htmllib, json, os, re, sys, threading, time
import urllib.parse, urllib.request
import xml.etree.ElementTree as ET
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

PORT = int(os.environ.get("PORT", "8000"))
VERSION = 4
HOSTED = any(os.environ.get(k) for k in ("RENDER", "RAILWAY_ENVIRONMENT", "FLY_APP_NAME", "K_SERVICE", "HOSTED"))
HOST = "0.0.0.0" if (HOSTED or "--lan" in sys.argv or os.environ.get("LAN")) else "127.0.0.1"
import base64, socket
ICON192 = base64.b64decode("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")
ICON512 = base64.b64decode("iVBORw0KGgoAAAANSUhEUgAAAgAAAAIACAIAAAB7GkOtAAAQAElEQVR4AezdC3ycVZnH8ZnckzZJaQu1tdCbBcHeFkq532+10JYqXRAUuYsuLuoCH8QVlY+ueFmFXS+7dhVFkTtIWSjSj6CAKAUVkP1QbmULal2RNsnkOslk9kibtkkzybwz533Pc87z81NJMvO+5zzP93k7/3ZmmlQ0n/J+fiGAAAIIKBSoSPE/BBBAAAGVAgSAyrHTNAKpVAoE9QIEgPpLAAAEENAqQABonTx9I4CAegECQO0lQOMIIKBdgADQfgXQPwIIqBUgANSOnsYRQECrwEDfBMCABB8RQAABZQIEgLKB0y4CCCAwIEAADEjwEQEtAvSJwDYBAmAbBB8QQAABbQIEgLaJ0y8CCCCwTYAA2Aah5wOdIoAAAlsFCICtDvwXAQQQUCdAAKgbOQ0jgIBWgaF9EwBDRfgaAQQQUCJAACgZNG0igAACQwUIgKEifI1AqAL0hcAQAQJgCAhfIoAAAloECAAtk6ZPBBBAYIgAATAEJNwv6QwBBBAYLEAADPbgKwQQQECNAAGgZtQ0igACWgUK9U0AFJLhdgQQQCBwAQIg8AHTHgIIIFBIgAAoJMPtCIQiQB8IFBAgAArAcDMCCCAQugABEPqE6Q8BBBAoIEAAFIAJ52Y6QQABBIYXIACGd+FWBBBAIHgBAiD4EdMgAghoFRitbwJgNCHuRwABBAIVIAACHSxtIYAAAqMJEACjCXE/Ar4KUDcCowgQAKMAcTcCCCAQqgABEOpk6QsBBBAYRYAAGAXI37upHAEEEBhZgAAY2Yd7EUAAgWAFCIBgR0tjCCCgVaDYvgmAYqU4DgEEEAhMgAAIbKC0gwACCBQrQAAUK8VxCPgiQJ0IFClAABQJxWEIIIBAaAIEQGgTpR8EEECgSAECoEgofw6jUgQQQKA4AQKgOCeOQgABBIITIACCGykNIYCAVoGofRMAUcU4HgEEEAhEgAAIZJC0gQACCEQVIACiinE8AlIFqAuBiAIEQEQwDkcAAQRCESAAQpkkfSCAAAIRBQiAiGByD6cyBBBAIJoAARDNi6MRQACBYAQIgGBGSSMIIKBVoNS+CYBS5TgPAQQQ8FyAAPB8gJSPAAIIlCpAAJQqx3kISBGgDgRKFCAASoTjNAQQQMB3AQLA9wlSPwIIIFCiAAFQIpyc06gEAQQQKE2AACjNjbMQQAAB7wUIAO9HSAMIIKBVoNy+CYByBTkfAQQQ8FSAAPB0cJSNAAIIlCtAAJQryPkIuBJgXwTKFCAAygTkdAQQQMBXAQLA18lRNwIIIFCmAAFQJqC709kZAQQQKE+AACjPj7MRQAABbwUIAG9HR+EIIKBVwFbfBIAtSdZBAAEEPBMgADwbGOUigAACtgQIAFuSrINAUgLsg4AlAQLAEiTLIIAAAr4JEAC+TYx6EUAAAUsCBIAlyOSWYScEEEDAjgABYMeRVRBAAAHvBAgA70ZGwQggoFXAdt8EgG1R1nMtMKE+t3BS78p9ej66f+eVB3Vcc1jHV45u/8YJme8tzty8tO0nK1p/urLlkTO3PPmBzS9c8Ldf5hPzpbnx7hWt5gBzmDnYnGJONKebRcxSZsHxdf2uO2N/BCwLEACWQVkuSYG6yv79JvQtmdnzD3/XaR6y71je+tTZmx8/q/WmpZnPH9Fxyf7d587tOX3fnmXvyJ4wrfewqb37T+rbd0JuenP/pIZ8U+22Ss0n5ktz434TcuYAc5g52JxiTjSnm0XMUmbBX72/Zd3ZW25b1valo9ovXtC5eEbPPuP7TAHbVuEDAh4KEAAeDk13yftP6jWPv/+1uO3nZ2x55tyWu1e0ff3Yjn88oNs8ZM/dPddYE6NOc01+/h59p87Ofnxh9/XHdax+T5spwJSx6qSMKckUFuPef1ua/yNgWYAAsAzKctYFqivy5rHVPMLe8O62352z+ealGfP4e8TUvslj89b3KmFBU8aRe/aakkxhvz17swmDC+d1zdu9rzItorwSOuIUPQIEgJ5Z+9Tpzg/65okX89hqHmEPfXtfQ5XoLsbUpEwYXLao6/blbU9+YAthIHpaFJdKEQDeXAVKCjV/dv7MoR2PnbnFlwf9QnPZOQweP2vL1Yd2zJ3YW+hgbkfAiQAB4ISdTYcKTB6TMy/kPriyxfzZ+cz9esbVpUL6n2nnrP167jg1Yxr8yIIu02xI3dGLvwIEgL+zC6HyMdX59+zdc+PJbQ+f0WJeyJ3WHPhbLU2Dly7sMs3+YEnbitk99VW8ThDCZRx/D3HtQADEJcu6Iwik0/kjp2a/dkzml2du+eKRHQdN7kun0yMcH9hd6XT64Cl91x7VYZ4auvao9oOn9BqQwHqkHS8ECAAvxhROkdObcpct6vzF+1pWLW4/eVZvfXU4rZXQSUN1asXs7A+WZAzIZQd2GJwSFuEUBEoWIABKpuPEaAIT6nP/tLDz9uWtF87rntTAUx+D9AzIhfN7DM4nFnYaqEH3pVJ8iUBMAgRATLAsu0NgamPuM4d2PHR660ULupsG/v3tjrv5bEDA4HxoQbeBuvrQDoM2cDMfEYhLgACIS5Z1jcDeu/V99ZjMgytbz9yvp072W/hNtUJ+Gaiz9usxaF85ut0ACqmKMoIUIADEj9XPAg+Y1PufJ2bufW/b0lm9lVxl0Ydo0Ja9I2sADeOciX3RF+AMBEYX4Lfm6EYcEUlgz8bcqpMyP16aOXov/t1TJLnhDzaMd57adt1xGQM7/BHcikCpAgRAqXKct4tAXVXevIZ532ktR+7JQ/8uOuXd8O4ZvQb24ws7DXJ5K3G2TwJx10oAxC2sZP38kpk9a05rMa9h1lYqekd/ktM1sBcv6DbIi2dkUyneRpWkfbB7EQDBjjaxxt45oe+2ZZmvH9sxRca350yscScbGeTrj2s34IbdSQFsGpIAARDSNJPuZXxd/zWHddy9vG3+Hn1J761hv8I9GnDD/rnDOpprA//mGYUNuMeCAAFgAVHhEpXp/Plzux78+5bT9+2p4CJycQUY9jP27XlwZcv73tltxuGiBPb0XoDfu96PMPkGFk3uvfe9rVcc1BXrj99Kvi8fdxxXl/rs4Z1mHGYoPtZPzW4FCAC3/iPsLvSu0/fp/s6JmVnjeOZB0IDMOFadlFk2q1tQTZTigwAB4MOUZNTYUNX/jRMy1xzRqfw7uMmYxtAq6qpSXzmm88tHtZsxDb2PrxEoIEAAFIDh5sECcyb03bOi9YRpvMF/sIuwr5bPzpoxmWEJq4tyIgokdTgBkJS0x/vkz53bfcuytr2aee+5B1M0YzLDOmdOlwe1UqJrAQLA9QRk799c0//dxZkrD+qsrpRdKNXtJGCG9cmDu8zzdWZ8O93MpwgMFSAAhorw9XaBAyb1/uQ9rYdP5T3+20mS+cTOLub5OjM+M0Q7y7FKiAIEQIhTtdHT4hk9Nyxpm8I/7rWB6WoNMz4zxCUze1wVwL7CBQgA4QNyU94H39V13bHttXxXHzf8Nnc1Q/zaMe3nz+UlAZuqwaxFAIgbpfOCPnlwx1WHdKXTfE8356OwU0A6nb7ioK7LF3XaWY5VAhIgAAIaZtmtVKTzXzii/Zw5PGNQNqW8BS6Y133N4e180wh5k3FZEQHgUl/U3nVV+f84IXPaPllRVVGMRYHT35n95vHtNRW8ndciqt2lkl6NAEhaXOZ+jTX9Pzq57ai9eMOPzPlYq+qYab2rFmeaavhOHtZIvV6IAPB6fHaKf1tD7ualbXN3z9lZjlVkCxw8pe9Hp7RNHsO4Zc8pkeoIgESYBW+yZ2PulmVts3fjj4Tuh5RYBfuM7//x0ra9msiAxMiFbkQACB1MMmXNmdh367LWybzZPxluSbtMGZu/5ZS2uRP55k6SppJ4LQRA4uRiNtyjIfetEzMT6sUURCHJCkxoyH/zxHZzGSS7LbsJEiAAxAwj2ULG1fZ/792ZSQ28ISRZd2G7mQvgu4vbzcUgrC7KSUiAAEgIWtQ2tZX5VYszPO8vaiiuitl7fG7VSRlzSbgqgH0dChAADvHdbF2Zzl9/bPs83vPjhl/irvP2yH39WP6NmMvRuNqbAHAl72zfLx/VcQw/18UZv9CNj5vW+8UjO4QWR1mxCRAAsdGKXPiyAztPeQf/1lfkbFwXtXx21lwerqtg/0QFCIBEud1u9sF3dV04n58b7nYIw+0u5jZzeZiLREw5FBK7AAEQO7GQDU6emb3yoC4hxVCGWAFzkSybxZ8SxM7HcmEEgGVQmcvNmdD3paPaK5i2zPFIqspcJP9yZOec3fkHYpKmElstPCTERlvswrEf11TTf/3x7dWVsW/EBmEImEvl+mPbzWUTRjt0MYIAATACTiB3fenojqmNfKufQKaZTBtTG/Nf5E1ByVg73YUAcMof/+bmNb1j9+Kv8/FDB7fD8dN7zcUTXFvCGnJdDgHgegJx7m+e+r98ES/8xkkc9Nrm4uHFgKAnnCIAgp2veQ6Xp/6DnW4ijfFiQCLMLjchAFzqx7o3T/3HymtjcQ/W4MUAD4ZURokEQBl4gk81z97y1L/g+fhUGi8G+DStiLUSABHBfDh80eRe8+ytD5VSox8C5nLixQA/RhWxSgIgIpi9w2NaaUL9376zo3n2Nqb1WVahgLmc+JcBQc6dAAhtrOfO6ZlYz495CW2szvsxLwZcxDeScj4G2wUQALZFna43o7nv7Dm879PpDMLd3FxaM8f1hdtfkp1J2YsAkDIJK3V86pDO2sq0laVYBIEhAubSuurgziE38qXXAgSA1+MbVPzx07JHTOUPaINM+MKugLnAzGVmd01WcyhAADjEt7n1mOr8pw7hJzrZJI1vLa9XNpdZTQUvMnk9wx3FEwA7LLz+7MMLuqaM5bel1zP0o3hzmV20gB8Y4MewRq2SABiVyIMDeO3XgyEFVOJF8zt5NTiMeRIAic8xhg0/exiv/cbAypIFBHg1uACMfzcTAP7NbEjFS2b2HDyF136HqPBlvAK8Ghyvb1KrEwBJScezT2U6f+kBvPE/HlxWHVHgsgO7zOU34iHcOVRA2tcEgLSJRKvnpBnZ6c38tK9oaBxtRWDGuJy5/KwsxSKuBAgAV/JW9s2fP4/3Y1iRZJFSBM6bay4/3ntWCp2QcwgAIYMopYzjp/XOmZgr5UzOcSIQ3KZzd8+ZFwOCa0tRQwSAx8Pmm3N5PLxQSr9wPi9BeTxLAsDX4S2a3Dt/D9784+v4gqn7oMl95lIMph1tjRAAiU3c8kYX8Scvy6IsV6IAl2KJcAJOIwAEDCF6Ce+a2Hf423ujn6f9jKZDr7X4S7vmQP/mUpwzkb+MDnB49ZEA8GpcA8V+ZEFXOs23fR7g4KNTgXQ6ff48XgkYcQZS7yQApE6mcF3TmvuOm5YtfD/3IJC0wInTs9Obcknvyn5lCxAAZRMmvsDF87vNn7kS35YNESgoUFWR/tAC/hJQ0EfsHQSA2NEMX9j4uv6ls/jj//A4Um9VUZe5LCfU85cAz2ZN6DkV2wAAEABJREFUAHg2sBNnZKsrPauZcjUImMvyhOm8McGzURMAng1syUz++O/ZyPSUy8Xp3awJgNhHZnGDPRtzi97GH7IsirKUTQFzce7FS8E2RWNfiwCIndjiBstnZ3n516InS9kVMBfnSTN67K7JarEKEACx8lpe/OSZ/O6yTMpydgVOnsnfUHcWlf45ASB9Qtvr23d838xx3nzrf4v/4NYstR0hyE9MgxZ/uSXad0LOXKhua2D34gUIgOKtHB+5Ym/++O94BGxfjAAXajFKQo4hAIQMYtQy8qfw/p9RkYQdoLOck2dlUyl+SowfwycA/JjTosl9Exr4TeXHsJRXObE+by5X5Qi+tE8A+DGpFbN5/sePSVGlEeByNQhe/CIAYhtTGQtv+t4VQ36dc841Fl8nTGCpMrof5lRbBQ+zdBk3hV1VOd2Zy3XIBRzpyzJmwqnRBAiAaF4cjQACCAQjQAAEM0oaQQABKQK+1EEA+DIp6kQAAQQsCxAAlkFZDgEEEPBFgADwZVLU6Y8AlSLgiQAB4MmgKBMBBBCwLUAA2BZlPQQQQMATAQLA+qBYEAEEEPBDgADwY05UiQACCFgXIACsk7IgAghoFfCtbwLAt4lRLwIIIGBJgACwBMkyCCCAgG8CBIBvE6NeuQJUhoBnAgSAZwOjXAQQQMCWAAFgS5J1EEAAAc8ECABrA2MhBBBAwC8BAsCveVEtAgggYE2AALBGyUIIIKBVwNe+CQBfJ0fdCCCAQJkCBECZgLGcPvm8L8+66Notj13Z9vjov+xWUMyOxRxDVcUobT1Gg5W5pIv/ZReE1UYQIABGwHF517Sm/kqG43ICEfbmUAQ8FeAxRujgZozLCa2MshBAIBQBAkDoJJtq80IroywEEAhFgAAoe5LxLNBY0x/PwqyKAAIIbBMgALZBSPvQXCOtIupBAIHQBAgAoRNtrOEpIKGjoSwEtgv4/gkBIHSCNZUEgNDRUBYCwQgQAEJHWVdFAAgdDWUhEIwAASB0lDwFJHQwO5fF5wh4LkAACB1gEy8CC50MZSEQjgABIHSWjbwNVOhkKAuBcAQIgJJnGe+J/EOweH1ZHQEEUikCQOhVUFPBi8BCR0NZCAQjQAAIHWUt7wISOhnKQiCVCsWAAAhlkvSBAAIIRBQgACKCJXV4JstokrJmHwS0CvAoI3Ty3X1poZVRVgoCBAIRIACEDjLLjwMQOhnKQiAcAQJA6Cx5CkjoYCgLgYAECIDIw0zmhLZsMvuwCwII6BUgAITOvofXAIROhrIQCEeAABA6yyw/EEzoZChLs0BovRMAQifa1sNohI6GshAIRoBHGaGjzGR5G6jQ0VAWAsEIEABCR9mdIwDEjYaCEAhMgAAQOtCeHN8MTuhoKAuBYAQIAKGj/GOmUmhllIUAAqEIEABFTzLZA19tIQCSFWc3BPQJEABCZ/56piLHO0GFDoeyEAhEgAAQOsj23opNHUxH6HQoS5tAqP3yECN3sn9sZzpyp0NlCAQgwEOM3CHyMoDc2VAZAkEIEAByx7ihhenImA5VIBCoAA8xcgf7x3beCCR3OlSGQAACBIDcIW5oZTpyp0NlCAQgwEPMqEN0dsD/tlbyTlBn+myMgAIBAkDukPvzad4JKnc8VIaA/wIEgOgZvsqzQKLnQ3GhC4TeHwEgesKPvl4tuj6KQwABnwUIANHTW7eJABA9IIpDwGsBAkD0+NZvqWzpFl1h0MXRHAKBCxAAogecz6ef+jN/CRA9I4pDwF8BAkD67NZtqpJeIvUhgICfAgRAwbkJuWMdLwMImQRlIBCcAAEgfaS8DCB9QtSHgLcCBID00fEygPQJUV+AAlpaIgA8mPQ6XgbwYEqUiIB/AgSABzNbx8sAHkyJEhHwT4AA8GBmvAyQ8JDYDgElAgSAB4M2LwM89FqNB4VSIgIIeCVAAPgxrvs31PpRKFUigIA/AgTALrMSecOv/lT11660yNIoCgEEfBUgAPyYXF9/+r5XeBbIj2FRJQK+CBAAvkwqdfeLPAvkzbAo1FMBbWUTAN5M/PnNVRtamJc386JQBOQL8IAif0Y7KryPl4J3YPAZAgiUK0AAlCuY5PmrX67J5/NJ7qhrL7pFQJkAAeDTwF9rq3z2DX48gE8jo1YEJAsQAJKnM0xt971CAAzDwk0IIFCCAAGwHc2PT/57Q01vzo9SqRIBBIQLEADCBzS0vDe7Ktdu5C8BQ1n4GgEEShAgAEpAc3zK95+rc1wB2yMQmIDWdggA/yb/zF+qn/lLlX91UzECCAgTIACEDaS4cr7/HP8quDgpjkIAgcICBEBhG8H3/PTVmk3tfG84WxNiHQSUChAAXg4+l0/f8HteCfBydhSNgBwBAkDOLKJVcuv62o2tjC8aGkcjgMDOAjyCpHbm8Ojz7lzFt5+u96hgSkUAAWkCBIC0iUSoZ/XLNfwlIIIXhyKAwGABAmCwh1dfmVcCrvsNfwnwamYUK0tAezUEgN9XwP18g2i/B0j1CLgUIABc6rM3Aggg4FCAAHCIz9aOBdgeAeUCBIDyC4D2EUBArwABoHf2dI4AAsoFFAeA8snTPgIIqBcgANRfAgAggIBWAQJA6+TpGwHFArS+VYAA2OrAfxFAAAF1AgSAupHTMAIIILBVgADY6sB/NQnQKwIIvCVAALzFwH8QQAABfQIEgL6Z0zECCCDwloDCAHirb/6DAAIIqBcgANRfAgAggIBWAQJA6+TpGwGFArQ8WIAAGOzBVwgggIAaAQJAzahpFAEEEBgsQAAM9uCrkAXorViBv3aliz2U43wWIAB8nh61IxCPwCceGhvPwqwqS4AAkDUPqkHAucBXn6x7YlO18zIoIAEBRQGQgCZbIOC7wKpn6lY90+B7F9RfpAABUCQUhyEQvsDNz9d89Uke/cMf9PYOCYDtFHyCgGqBhzdWf+7xMYES0NbwAgTA8C7cioAqgSc2VV360Nh8njf/qBp7igDQNW+6RWBXgWffqPzwg409OR79d7UJ/BYCIPAB014qlQJhBIGXtlRc+EBjRy+P/iMgBXsXARDsaGkMgVEFXm+rOG9NY0sPjwOjUoV5AIMPc650hcCoAlu60ueuafxLZ+WoR3JAqAIKAiDU0dEXAmUIbGpPr1zd9HqGR/8yEP0/lQDwf4Z0gEBEAfO8/xk8+kdEC/JwAiDIsdIUAgUFnthU9b57m/6s45mfggrc8ZYAAfAWA/9BQIfAL16rumBNYybLb3wd8x6tS66D0YSU3X/r+hplHStq944Xai5e25jt5x2fioY+cqsEwMg+6u69+rGx1z1VH0jbtLGTwL/9pu5Tj47t59/67mTCpwQA18BQgW8/Xf/pxxry+fzQO/jaT4Fcf+qqRxq++Tu+y5uf84uzagIgTl1v175tfd0nHh7bkyMDvB3hQOFmiBc/OPbOF+sGbuAjAjsEAg6AHU3yWQkC92+oPXN185/aeb64BDwpp7zWmjZDfOQPvK4jZSLS6iAApE1EUD3PvVl16l3Nazfyw6EEDaX4Uszglt/dbIZY/CkcqU2AANA28Wj9tmYrLlnbaF4S6OqNdiJHOxQww7r60QYzuM4+rb/BHep7tTXXh1fjclSseUngtNVN5vkER/uzbQQBMyYzrFtf4En/CGhqDyUA1I4+WuMvb6kyzyfc8xLPJkdzS/5oMyYzrOT3ZUcfBQgAH6fmpmbzfMIVvxh7+cMN3X1uCih6V9UHmjGp7p/mowgQAFG0ODaVWv1K3XvvaXppC1cOVwMC3gvw29j7ESbfgHmGYfldzV/4dUMmm/zm7IgAAtYEAgwAazYsVFggl0/f+Fzd4tvHrXmFVwUKM3EPArIFCADZ85Fd3V+7Kj728NgP3t/IM0KyB0V1CAwvQAAM78KtxQv8+k/VPCNUPBdHxinA2tEECIBoXhw9rADPCA3Lwo0ICBcgAIQPyKfyeEbIp2lRKwKpFAHAVWBZwDwjtOyuZsuLFrccRyGAQCQBAiASFwcXJcBPHSmKiYMQcC1AALieAPsjgAACjgQCCgBHgmyLAAIIeCpAAHg6OMpGAAEEyhUgAMoV5HwEShbIZFN3vsg/pS7Zb8eJfFaaAAFQmhtnIVC6QDaXWrux+tKfjTnspt2uemRs6QtxJgLlCRAA5flxNgJFC+Tz+XWbqj79WMPhN427ZG3jA6/W9uTSRZ/NgQjYFyAA7JuyYtIC4vd7paXiuqfqj7ll3Afua7ptfV1rlt934memo0AuRB1zpsvEBd7sSq15pebqxxpOvL1pyR3jvv10/aaOysSrYEMERhIgAEbS4T4hAqfc0fz5XzX8bGN1W4+QioYvw5RnivzCrxuW3tl06E27fezhsbeur9vYWjX80dyKgGuBAALANSH7xy/wUkvlD/+n7iNrGw/84fiV9zT965P1j/6halO7iCfQTRmmGFOSKcyUZ4q88bm6F7eYB30R5cU/HHbwWIAA8Hh4Okt/9o2q7zxTf8EDTUffstv8G8adelfTxx4ac/1T9atfrnn2jcpMgj+kbPndTaYAU4YpxpRkCtM5Ebr2V4AA8Hd2VJ7qzlU8v7lqzYbabz1df/nPx668p3nhjeMP+dG4s+5t/OdHx3zjt3U3/L721udrTTas3Vj9yz9U2yVb/2aVKcDumqwWTYCjyxMgAMrz42x5Apu7K576v+rbX6j99982XPvEmKt/OcZkwyVrG897oFFesVSEgEsBAsClPnsjgAACDgUIAIf4bF2mAKcjgEBZAgRAWXycjAACCPgrQAD4OzsqRwABBMoS8DgAyuqbkxFAAAH1AgSA+ksAAAQQ0CpAAGidPH0j4LEApdsRIADsOLIKAggg4J0AAeDdyCgYAQQQsCNQcdvC1/nltYCdC2FgFVsUA+vZ+TikqpK/tFPNwCollzHkxIH17HwcsnjJX9qpZmCVksvgxFgF+BvAwBXKRwQQQECZAAGgbOC0iwACCAwIeBgAA6XzEQEEEECgHAECoBw9zkUAAQQ8FiAAPB4epSOgTYB+7QoQAHY9WQ0BBBDwRoAA8GZUFIoAAgjYFSAA7HqyWpwCrI0AAlYFCACrnCyGAAII+CNAAPgzKypFAAEErAp4FABW+2YxBBBAQL0AAaD+EgAAAQS0ChAAWidP3wh4JECp8QgQAPG4sioCCCAgXoAAED8iCkQAAQTiESAA4nFlVZsCrIUAArEIEACxsLIoAgggIF+AAJA/IypEAAEEYhHwIABi6ZtFEUAAAfUCBID3l8Cj3/qoxV+2OCyWZJaiKoNQ5K+wrWx1xzpbBQiArQ78FwEEBApQUrwCBEC8vqyOAAIIiBUgAMSOhsIQQACBeAUIgHh9Wb0cAc5FAIFYBQiAWHlZHAEEEJArQADInQ2VIYAAArEKCA6AWPtmcQQQQEC9AAGg/hIAAAEEtAoQAFonT98ICBagtGQEKqZPn8ovBBBAAAGFAvwNIJmgZRcEEEBAnAABIG4kFJSCAAEEEhEgABJhZqfIKt0AAAbmSURBVBMEEEBAngABIG8mVIQAAggkIiAwABLpm00QQAAB9QIEgPpLAAAEENAqQABonTx9IyBQgJKSFSAAkvVmNwQQQECMAAEgZhQUggACCCQrQAAk681uIwlwHwIIJCpAACTKzWYIIICAHAECQM4sqAQBBBBIVEBQACTaN5shgAAC6gUIAPWXAAAIIKBVgADQOnn6RkCQAKW4ESAA3LizKwIIIOBcgABwPgIKQAABBNwIEABu3Nl1ZwE+RwABJwIEgBN2NkUAAQTcCxAA7mdABQgggIATAQEB4KRvNkUAAQTUCxAA6i8BABBAQKsAAaB18vSNgAABSnArQAC49Wd3BBBAwJkAAeCMno0RQAABtwIEgFt/3bvTPQIIOBUgAJzyszkCCCDgToAAcGfPzggggIBTAYcB4LRvNkcAAQTUCxAA6i8BABBAQKsAAaB18vSNgEMBtpYhQADImANVIIAAAokLEACJk7MhAgggIEOAAJAxB11V0C0CCIgQIABEjIEiEEAAgeQFCIDkzdkRAQQQECHgIABE9E0RCCCAgHoBAkD9JQAAAghoFSAAtE6evhFwIMCWsgQIAFnzoBoEEEAgMQECIDFqNkIAAQRkCRAAsuYRdjV0hwACogQIAFHjoBgEEEAgOQECIDlrdkIAAQRECSQYAKL6phgEEEBAvQABoP4SAAABBLQKEABaJ0/fCCQowFYyBQgAmXOhKgQQQCB2AQIgdmI2QAABBGQKEAAy5xJWVXSDAAIiBQgAkWOhKAQQQCB+AQIgfmN2QAABBEQKJBAAIvumKAQQQEC9AAGg/hIAAAEEtAoQAFonT98IJCDAFrIFCADZ86E6BBBAIDYBAiA2WhZGAAEEZAsQALLn43d1VI8AAqIFCADR46E4BBBAID4BAiA+W1ZGAAEERAvEGACi+6Y4BBBAQL0AAaD+EgAAAQS0ChAAWidP3wjEKMDSfggQAH7MiSoRQAAB6wIEgHVSFkQAAQT8ECAA/JiTX1VSLQIIeCFAAHgxJopEAAEE7AsQAPZNWREBBBDwQiCGAPCib4pEAAEE1AsQAOovAQAQQECrAAGgdfL0jUAMAizplwAB4Ne8qBYBBBCwJkAAWKNkIQQQQMAvAQLAr3nJrpbqEEDAKwECwKtxUSwCCCBgT4AAsGfJSggggIBXAhYDwKu+KRYBBBBQL0AAqL8EAEAAAa0CBIDWydM3AhYFWMpPAQLAz7lRNQIIIFC2AAFQNiELIIAAAn4KEAB+zk1W1VSDAAJeChAAXo6NohFAAIHyBQiA8g1ZAQEEEPBSwEIAeNk3RSOAAALqBQgA9ZcAAAggoFWAANA6efpGwIIAS/gtQAD4PT+qRwABBEoWIABKpuNEBBBAwG8BAsDv+bmtnt0RQMBrAQLA6/FRPAIIIFC6AAFQuh1nIoAAAl4LlBEAXvdN8QgggIB6AQJA/SUAAAIIaBUgALROnr4RKEOAU8MQIADCmCNdIIAAApEFCIDIZJyAAAIIhCFAAIQxx2S7YDcEEAhCgAAIYow0gQACCEQXIACim3EGAgggEIRACQEQRN80gQACCKgXIADUXwIAIICAVgECQOvk6RuBEgQ4JSwBAiCsedINAgggULQAAVA0FQcigAACYQkQAGHNM95uWB0BBIISIACCGifNIIAAAsULEADFW3EkAgggEJRAhAAIqm+aQQABBNQLEADqLwEAEEBAqwABoHXy9I1ABAEODVOAAAhzrnSFAAIIjCpAAIxKxAEIIIBAmAIEQJhztdsVqyGAQJACBECQY6UpBBBAYHQBAmB0I45AAAEEghQoIgCC7JumEEAAAfUCBID6SwAABBDQKkAAaJ08fSNQhACHhC1AAIQ9X7pDAAEECgoQAAVpuAMBBBAIW4AACHu+5XXH2QggELQAARD0eGkOAQQQKCxAABS24R4EEEAgaIERAiDovmkOAQQQUC9AAKi/BABAAAGtAgSA1snTNwIjCHCXDgECQMec6RIBBBDYRYAA2IWEGxBAAAEdAgSAjjlH65KjEUBAhQABoGLMNIkAAgjsKkAA7GrCLQgggIAKgWECQEXfNIkAAgioFyAA1F8CACCAgFYBAkDr5OkbgWEEuEmXAAGga950iwACCGwXIAC2U/AJAgggoEuAANA175G75V4EEFAlQACoGjfNIoAAAjsECIAdFnyGAAIIqBLYKQBU9U2zCCCAgHoBAkD9JQAAAghoFSAAtE6evhHYSYBPdQoQADrnTtcIIIBAigDgIkAAAQSUChAASgc/qG2+QAABlQIEgMqx0zQCCCCQ4ikgLgIEEEBAq0BFKqW1dfpGAAEEdAvwFJDu+dM9AggoFiAAFA+f1hFAQLfA/wMAAP//LNEmbwAAAAZJREFUAwDl3bG1lEek/QAAAABJRU5ErkJggg==")
MANIFEST = json.dumps({"name": "رصد الأقصر", "short_name": "رصد الأقصر", "lang": "ar", "dir": "rtl", "start_url": "/", "scope": "/", "display": "standalone", "background_color": "#0f4c5c", "theme_color": "#0f4c5c", "icons": [{"src": "/icon-192.png", "sizes": "192x192", "type": "image/png", "purpose": "any maskable"}, {"src": "/icon-512.png", "sizes": "512x512", "type": "image/png", "purpose": "any maskable"}]}, ensure_ascii=False)
SW = "self.addEventListener('install',e=>self.skipWaiting());self.addEventListener('activate',e=>self.clients.claim());self.addEventListener('fetch',e=>{});"
HERE = os.path.dirname(os.path.abspath(__file__))
DB_FILE = os.path.join(HERE, "luxor_news_db.json")
UPDATE_FILE = os.path.join(HERE, "update_url.txt")
REFRESH_MINUTES = 15
KEEP_DAYS = 30
UA = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124 Safari/537.36"
TEST = bool(os.environ.get("LUXOR_TEST"))

# لغة: (عبارة البحث, hl, gl, ceid)
LANGS = {
    "ar": ("الأقصر", "ar", "EG", "EG:ar"),
    "en": ("Luxor Egypt", "en", "EG", "EG:en"),
    "fr": ("Louxor Égypte", "fr", "FR", "FR:fr"),
    "de": ("Luxor Ägypten", "de", "DE", "DE:de"),
    "es": ("Luxor Egipto", "es", "ES", "ES:es"),
    "it": ("Luxor Egitto", "it", "IT", "IT:it"),
    "ru": ("Луксор Египет", "ru", "RU", "RU:ru"),
    "tr": ("Luksor Mısır", "tr", "TR", "TR:tr"),
    "zh": ("卢克索 埃及", "zh-CN", "CN", "CN:zh-Hans"),
    "ja": ("ルクソール エジプト", "ja", "JP", "JP:ja"),
}
AR_TOPICS = ["", "سياحة", "آثار", "حادث", "حريق", "شكوى", "استغاثة", "فيديو متداول", "مشروع", "محافظ الأقصر",
             "مستشفى", "مدارس", "كهرباء مياه", "رياضة", "أسوان إسنا", "أرمنت", "الكرنك", "البر الغربي"]
EN_TOPICS = ["tourism", "archaeology discovery", "Karnak", "Valley of the Kings", "hotel Nile cruise", "accident", "governor"]
AR_SITES = ["youm7.com", "almasryalyoum.com", "ahram.org.eg", "gate.ahram.org.eg", "masrawy.com", "shorouknews.com",
            "elwatannews.com", "cairo24.com", "elbalad.news", "elfagr.org", "dostor.org", "vetogate.com", "albawabhnews.com",
            "akhbarelyom.com", "alwafd.news", "almshhdnews.com", "cairomubasher.com", "elnabaa.net", "egypttelegraph.com",
            "mobtada.com", "elwatan.com", "elaosboa.com", "akhbarak.net", "alarabiya.net", "aljazeera.net", "bbc.com/arabic",
            "aawsat.com", "skynewsarabia.com", "rt.com", "dw.com/ar", "france24.com/ar", "arabic.cnn.com", "elzmannews.com",
            "ahlmasrnews.com", "mwq3masr.com", "luxor.gov.eg", "sis.gov.eg", "cabinet.gov.eg", "alqahera.com", "elmwatin.com"]
EN_SITES = ["english.ahram.org.eg", "egyptindependent.com", "egypttoday.com", "dailynewsegypt.com", "egyptian-gazette.com",
            "egyptianstreets.com", "bbc.com", "cnn.com", "reuters.com", "apnews.com", "theguardian.com", "nytimes.com",
            "aljazeera.com", "thenationalnews.com", "arabnews.com", "nationalgeographic.com", "archaeology.org",
            "smithsonianmag.com", "livescience.com", "arkeonews.net", "travelandtourworld.com", "traveldailynews.com",
            "lonelyplanet.com", "cntraveler.com", "egymonuments.gov.eg", "euronews.com", "dw.com", "france24.com"]

AR_RE = re.compile(r"[؀-ۿ]")
LUXOR_AR = ["الأقصر", "الاقصر", "أرمنت", "ارمنت", "إسنا", "اسنا", "القرنة", "الكرنك", "البياضية", "الطود", "وادي الملوك", "طيبة", "الزينية", "العديسات"]
LUXOR_LATIN = ["luxor", "louxor", "luksor", "луксор", "卢克索", "ルクソール", "karnak", "valley of the kings"]
EGYPT_HINT = ["egypt", "égypte", "egypte", "ägypten", "egipto", "egitto", "египет", "mısır", "埃及", "エジプト", "karnak", "nile",
              "pharaoh", "pharaon", "pharao", "faraón", "faraone", "valley of the kings", "nil ", "nilo", "нил", "карнак", "ptolem"]
COMPLAINT = ["استغاثة", "يستغيث", "استغاثه", "شكوى", "شكاوى", "مناشدة", "يناشد", "فيديو متداول", "فيديو استغاثة", "أهالي", "يشكو", "يشكون"]
SOCIAL = ["فيديو متداول", "رواد مواقع التواصل", "السوشيال", "سوشيال ميديا", "فيسبوك", "تيك توك", "إنستجرام", "منشور", "تريند",
          "viral", "tiktok", "facebook", "instagram", "social media"]

_lock = threading.Lock()
_run_lock = threading.Lock()
STATE = {"items": {}, "status": {"running": False, "lastRun": None, "lastNew": 0, "sources": 0, "failed": 0, "total": 0}}


def log(*a):
    print(time.strftime("%H:%M:%S"), *a, flush=True)


def http_get(url):
    if TEST:
        return TEST_FEED.encode("utf-8")
    req = urllib.request.Request(url, headers={"User-Agent": UA, "Accept": "application/rss+xml, application/atom+xml, application/xml, text/xml, */*"})
    with urllib.request.urlopen(req, timeout=10) as r:
        return r.read()


def strip_tags(s):
    return re.sub(r"\s+", " ", re.sub(r"<[^>]+>", " ", htmllib.unescape(s or ""))).strip()


def local(tag):
    return tag.rsplit("}", 1)[-1].lower()


def parse_date(s):
    if not s:
        return None
    s = s.strip()
    try:
        d = parsedate_to_datetime(s)
    except Exception:
        try:
            d = datetime.fromisoformat(s.replace("Z", "+00:00"))
        except Exception:
            return None
    if d.tzinfo is None:
        d = d.replace(tzinfo=timezone.utc)
    return d.astimezone(timezone.utc)


def parse_feed(data):
    """يقرأ RSS أو Atom ويعيد قائمة عناصر خام."""
    root = ET.fromstring(data)
    out = []
    for el in root.iter():
        t = local(el.tag)
        if t not in ("item", "entry"):
            continue
        d = {"title": "", "link": "", "date": "", "desc": "", "source": ""}
        for c in el:
            k = local(c.tag)
            txt = (c.text or "").strip()
            if k == "title":
                d["title"] = strip_tags(txt)
            elif k == "link":
                d["link"] = c.attrib.get("href") or txt
            elif k in ("pubdate", "published", "updated", "date") and not d["date"]:
                d["date"] = txt
            elif k in ("description", "summary", "content"):
                d["desc"] = strip_tags(txt)
            elif k == "source":
                d["source"] = strip_tags(txt)
        if d["title"] and d["link"]:
            out.append(d)
    return out


def relevant(text):
    low = text.lower()
    if AR_RE.search(text):
        return any(w in text for w in LUXOR_AR)
    if not any(w in low for w in LUXOR_LATIN) and not any(w in text for w in LUXOR_LATIN):
        return False
    return any(h in low or h in text for h in EGYPT_HINT)


SOCIAL_SRC = ("Reddit", "Mastodon", "Bluesky", "Telegram", "YouTube")


def make_item(raw, qlang, default_source, trusted=False):
    title, desc = raw["title"], raw["desc"]
    source = raw["source"] or default_source
    if source and title.endswith(" - " + source):
        title = title[: -len(source) - 3].strip()
    elif " - " in title and not raw["source"] and default_source.startswith("Google"):
        title, _, tail = title.rpartition(" - ")
        source = tail.strip() or source
    if desc and (desc.startswith(title) or title.startswith(desc[:40])):
        desc = ""
    desc = desc[:240]
    text = title + " " + desc
    if not trusted and not relevant(text):
        return None
    d = parse_date(raw["date"])
    lang = "ar" if AR_RE.search(title) else (qlang if qlang != "ar" else "en")
    item = {
        "lang": lang,
        "source": source or "مصدر",
        "title": title,
        "summary": desc,
        "date": d.isoformat().replace("+00:00", "Z") if d else None,
        "url": raw["link"],
        "cat": "complaint" if any(w in text for w in COMPLAINT) else "",
        "social": any(w in text.lower() for w in SOCIAL) or default_source in SOCIAL_SRC or source.startswith(SOCIAL_SRC),
        "tags": [],
    }
    if item["cat"]:
        item["tags"].append("شكوى أو استغاثة")
    if item["social"]:
        item["tags"].append("متداول على السوشيال")
    return item


def gnews(q, hl, gl, ceid):
    return "https://news.google.com/rss/search?q=%s&hl=%s&gl=%s&ceid=%s" % (urllib.parse.quote(q), hl, gl, ceid)


SOCIAL_AR = ["فيسبوك", "تيك توك", "فيديو متداول", "منشور متداول", "رواد مواقع التواصل", "استغاثة مواطن", "مناشدة المحافظ",
             "شكوى أهالي", "انقطاع المياه", "انقطاع الكهرباء", "قمامة", "مواطنون يشكون", "نداء", "تريند", "سوشيال ميديا",
             "مواطن يستغيث", "سرقة", "تعدي", "حادث طريق", "مياه الشرب", "صرف صحي", "إهمال مستشفى", "غضب", "أزمة"]
SOCIAL_EN = ["viral video", "tiktok", "facebook post", "tourists complaint", "tourist scam", "tripadvisor warning", "instagram"]
SUBREDDITS = ["Egypt", "travel", "solotravel", "Egyptology", "ancientegypt", "AskEgypt"]
MASTO = ["mastodon.social", "mstdn.social", "mastodon.online", "masto.ai"]
EXTRA_FILE = os.path.join(HERE, "sources.txt")
EXTRA_HELP = """# مصادر إضافية يحددها المستخدم، سطر لكل مصدر (السطور التي تبدأ بعلامة # تُهمل)
# قناة تيليجرام عامة:    telegram:اسم_القناة
# قناة يوتيوب:          youtube:معرف_القناة
# رابط RSS لأي موقع:    rss:https://example.com/feed
# أضف كلمة all في آخر السطر لقبول كل منشورات المصدر دون فلترة (مناسبة لقنوات الأقصر المتخصصة)، مثال:
# telegram:اسم_قناة_عن_الاقصر all
"""


def extra_tasks():
    T = []
    try:
        if not os.path.exists(EXTRA_FILE):
            open(EXTRA_FILE, "w", encoding="utf-8").write(EXTRA_HELP)
            return T
        for line in open(EXTRA_FILE, encoding="utf-8"):
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            all_ = line.endswith(" all")
            if all_:
                line = line[:-4].strip()
            kind, _, val = line.partition(":")
            val = val.strip()
            if kind == "telegram" and val:
                T.append(("https://t.me/s/" + val.lstrip("@"), "ar", "Telegram", all_))
            elif kind == "youtube" and val:
                T.append(("https://www.youtube.com/feeds/videos.xml?channel_id=" + val, "ar", "YouTube", all_))
            elif kind == "rss" and val:
                T.append((val, "ar", "RSS", all_))
    except Exception:
        pass
    return T


def build_tasks():
    T = []  # (url, lang, default_source, trusted)
    G = lambda q, hl, gl, ceid, lang, src="Google News": T.append((gnews(q, hl, gl, ceid), lang, src, False))
    B = lambda q, hl, lang: T.append(("https://www.bing.com/news/search?q=%s&format=rss&setlang=%s" % (urllib.parse.quote(q), hl), lang, "Bing News", False))
    for code, (term, hl, gl, ceid) in LANGS.items():
        G(term + " when:7d", hl, gl, ceid, code)
        B(term, hl, code)
    for t in AR_TOPICS[1:]:
        G("الأقصر %s when:7d" % t, "ar", "EG", "EG:ar", "ar")
        B("الأقصر " + t, "ar", "ar")
    for t in SOCIAL_AR:
        G("الأقصر %s when:14d" % t, "ar", "EG", "EG:ar", "ar")
        B("الأقصر " + t, "ar", "ar")
    for t in EN_TOPICS:
        G("Luxor Egypt %s when:14d" % t, "en", "US", "US:en", "en")
    for t in SOCIAL_EN:
        G("Luxor Egypt %s when:30d" % t, "en", "US", "US:en", "en")
        B("Luxor Egypt " + t, "en", "en")
    for s_ in AR_SITES:
        G("الأقصر site:%s when:7d" % s_, "ar", "EG", "EG:ar", "ar")
    for s_ in EN_SITES:
        G("Luxor Egypt site:%s when:14d" % s_, "en", "US", "US:en", "en")
    for q in ("Luxor Egypt", "الأقصر", "Luxor temple", "Karnak"):
        T.append(("https://www.reddit.com/search.rss?q=%s&sort=new&t=month" % urllib.parse.quote(q), "en", "Reddit", False))
    for sr in SUBREDDITS:
        for q in ("Luxor", "الأقصر"):
            T.append(("https://www.reddit.com/r/%s/search.rss?q=%s&restrict_sr=1&sort=new&t=month" % (sr, urllib.parse.quote(q)), "en", "Reddit", False))
    for inst in MASTO:
        for tag in ("luxor", "الأقصر", "الاقصر", "karnak"):
            T.append(("https://%s/tags/%s.rss" % (inst, urllib.parse.quote(tag)), "en", "Mastodon", False))
    for q in ("الأقصر", "الاقصر", "Luxor Egypt", "Luxor temple", "Karnak"):
        T.append(("https://public.api.bsky.app/xrpc/app.bsky.feed.searchPosts?q=%s&sort=latest&limit=50" % urllib.parse.quote(q), "en", "Bluesky", False))
    T.extend(extra_tasks())
    return T


def parse_bsky(data):
    out = []
    for p in json.loads(data).get("posts", []):
        rec = p.get("record", {})
        text = re.sub(r"\s+", " ", rec.get("text", "")).strip()
        handle = p.get("author", {}).get("handle", "")
        if text and handle:
            out.append({"title": text[:140], "desc": text, "date": rec.get("createdAt", ""),
                        "link": "https://bsky.app/profile/%s/post/%s" % (handle, p.get("uri", "").rsplit("/", 1)[-1]),
                        "source": "Bluesky @" + handle})
    return out


def parse_telegram(html, url):
    chan = url.rsplit("/", 1)[-1]
    out = []
    for b in html.split("tgme_widget_message_wrap")[1:]:
        m = re.search(r'data-post="([^"]+)"', b)
        t = re.search(r'tgme_widget_message_text[^>]*>(.*?)</div>', b, re.S)
        d = re.search(r'<time[^>]*datetime="([^"]+)"', b)
        if not (m and t):
            continue
        text = strip_tags(t.group(1))
        if text:
            out.append({"title": text[:140], "desc": text, "date": d.group(1) if d else "",
                        "link": "https://t.me/" + m.group(1), "source": "Telegram @" + chan})
    return out


def run_task(task):
    url, qlang, default_source, trusted = task if len(task) == 4 else task + (False,)
    for attempt in range(2):
        try:
            data = http_get(url)
            if default_source == "Bluesky":
                raws = parse_bsky(data)
            elif default_source == "Telegram":
                raws = parse_telegram(data.decode("utf-8", "ignore"), url)
            else:
                raws = parse_feed(data)
            items = []
            for raw in raws:
                it = make_item(raw, qlang, default_source, trusted)
                if it:
                    items.append(it)
            return items, True
        except urllib.error.HTTPError as e:
            if e.code < 500:
                break
        except Exception:
            pass
        time.sleep(0.5)
    return [], False


def item_id(it):
    key = re.sub(r"\W+", "", it["title"].lower())[:90]
    return hashlib.sha1(key.encode("utf-8")).hexdigest()[:12]


def load_db():
    try:
        with open(DB_FILE, encoding="utf-8") as f:
            d = json.load(f)
        STATE["items"] = d.get("items", {})
        STATE["status"].update(d.get("status", {}))
        STATE["status"]["running"] = False
    except Exception:
        pass


def save_db():
    tmp = DB_FILE + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump({"items": STATE["items"], "status": STATE["status"]}, f, ensure_ascii=False)
    os.replace(tmp, DB_FILE)


def refresh_once():
    """يجمع الأخبار من كل المصادر بالتوازي، وتظهر النتائج أولا بأول."""
    if not _run_lock.acquire(blocking=False):
        return None
    try:
        tasks = build_tasks()
        st = STATE["status"]
        st.update({"running": True, "done": 0, "sources": len(tasks), "lastNew": 0})
        log("بدء الجمع من", len(tasks), "مصدر")
        with _lock:
            urls = {v["url"] for v in STATE["items"].values()}
        new = ok = 0
        with ThreadPoolExecutor(max_workers=24) as ex:
            futs = [ex.submit(run_task, t) for t in tasks]
            for f in as_completed(futs):
                items, good = f.result()
                ok += 1 if good else 0
                with _lock:
                    for it in items:
                        i = item_id(it)
                        if i in STATE["items"] or it["url"] in urls:
                            continue
                        it["id"] = i
                        it["seen"] = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
                        STATE["items"][i] = it
                        urls.add(it["url"])
                        new += 1
                    st["done"] += 1
                    st["lastNew"] = new
                    st["total"] = len(STATE["items"])
        cutoff = datetime.now(timezone.utc) - timedelta(days=KEEP_DAYS)
        with _lock:
            for i in list(STATE["items"]):
                d = parse_date(STATE["items"][i].get("date"))
                if d and d < cutoff:
                    del STATE["items"][i]
            st.update({"lastRun": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"), "lastNew": new,
                       "failed": len(tasks) - ok, "total": len(STATE["items"])})
            save_db()
        log("انتهى الجمع: جديد", new, "| الإجمالي", len(STATE["items"]), "| مصادر فاشلة", len(tasks) - ok)
        return new
    finally:
        STATE["status"]["running"] = False
        _run_lock.release()


def snapshot():
    with _lock:
        items = sorted(STATE["items"].values(), key=lambda x: x.get("date") or "", reverse=True)
        st = dict(STATE["status"])
        st["version"] = VERSION
        return {"news": items, "status": st}


def background():
    while True:
        check_update()
        try:
            refresh_once()
        except Exception as e:
            log("خطأ في الجمع:", e)
        time.sleep(REFRESH_MINUTES * 60)


def update_url():
    u = os.environ.get("UPDATE_URL", "").strip()
    if not u:
        try:
            u = open(UPDATE_FILE, encoding="utf-8").read().strip()
        except Exception:
            u = ""
    return u


def check_update():
    """يفحص رابط التحديث، وإن وجد نسخة أحدث يستبدل الملف ويعيد التشغيل."""
    u = update_url()
    if not u or HOSTED:
        return
    try:
        with urllib.request.urlopen(urllib.request.Request(u, headers={"User-Agent": UA, "Cache-Control": "no-cache"}), timeout=20) as r:
            code = r.read().decode("utf-8")
        m = re.search(r"^VERSION = (\d+)", code, re.M)
        if not m or int(m.group(1)) <= VERSION:
            return
        compile(code, "luxor_news_app.py", "exec")
        me = os.path.abspath(__file__)
        with open(me + ".new", "w", encoding="utf-8") as f:
            f.write(code)
        os.replace(me + ".new", me)
        log("تم تنزيل نسخة أحدث (%s)، جار إعادة التشغيل" % m.group(1))
        save_db()
        os.execv(sys.executable, [sys.executable, me] + sys.argv[1:])
    except Exception as e:
        log("تعذر فحص التحديث:", e)


class Quiet(ThreadingHTTPServer):
    daemon_threads = True

    def handle_error(self, request, client_address):
        pass


class H(BaseHTTPRequestHandler):
    def log_message(self, *a):
        pass

    def send(self, code, body, ctype):
        b = body if isinstance(body, bytes) else body.encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(b)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        try:
            self.wfile.write(b)
        except (BrokenPipeError, ConnectionResetError):
            pass

    def do_GET(self):
        p = self.path.split("?")[0]
        if p == "/":
            self.send(200, PAGE, "text/html; charset=utf-8")
        elif p == "/manifest.webmanifest":
            self.send(200, MANIFEST, "application/manifest+json; charset=utf-8")
        elif p == "/sw.js":
            self.send(200, SW, "application/javascript")
        elif p == "/icon-192.png":
            self.send(200, ICON192, "image/png")
        elif p == "/icon-512.png":
            self.send(200, ICON512, "image/png")
        elif p == "/api/news":
            self.send(200, json.dumps(snapshot(), ensure_ascii=False), "application/json; charset=utf-8")
        else:
            self.send(404, "not found", "text/plain")

    def do_POST(self):
        if self.path.split("?")[0] == "/api/refresh":
            started = False
            lr = parse_date(STATE["status"].get("lastRun"))
            recent = bool(lr and (datetime.now(timezone.utc) - lr).total_seconds() < 60)
            if not _run_lock.locked() and not recent:
                STATE["status"]["running"] = True
                STATE["status"]["done"] = 0

                def job():
                    check_update()
                    refresh_once()
                threading.Thread(target=job, daemon=True).start()
                started = True
            snap = snapshot()
            snap["started"] = started
            self.send(200, json.dumps(snap, ensure_ascii=False), "application/json; charset=utf-8")
        else:
            self.send(404, "not found", "text/plain")


def export_files():
    snap = snapshot()["news"]
    with open(os.path.join(HERE, "luxor_news.json"), "w", encoding="utf-8") as f:
        json.dump(snap, f, ensure_ascii=False, indent=1)
    with open(os.path.join(HERE, "luxor_news.csv"), "w", encoding="utf-8-sig", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["date", "lang", "source", "title", "summary", "url", "cat", "social"], extrasaction="ignore")
        w.writeheader()
        w.writerows(snap)


PAGE = r'''<!doctype html><html lang="ar" dir="rtl"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1,viewport-fit=cover"><title>رصد الأقصر</title><link rel="manifest" href="/manifest.webmanifest"><link rel="icon" href="/icon-192.png"><link rel="apple-touch-icon" href="/icon-192.png"><meta name="theme-color" content="#0f4c5c"><meta name="mobile-web-app-capable" content="yes"><meta name="apple-mobile-web-app-capable" content="yes">

<link rel="stylesheet" href="https://fonts.googleapis.com/css2?family=Noto+Sans+Arabic:wght@400;500;600;700&display=swap">
<style>
/* Layout: Nile-at-dusk header scene (pylon, obelisk, palms, sun) over a single feed column; fixed three-button bottom bar. Gold only on the sun and the active tab marker. */
:root{
  --bg:#EDEBE6; --surface:#FBFAF7; --ink:#0E2A47; --muted:#5B6B7A; --line:#DAD6CC;
  --nile:#0E2A47; --nile-2:#17476E; --accent:#16808C; --accent-soft:#D8ECEE;
  --gold:#D9A441; --on-nile:#F4EFE3; --danger:#9C3B2E;
  --display:'Noto Sans Arabic','Segoe UI',Tahoma,sans-serif;
  --body:'Noto Sans Arabic','Segoe UI',Tahoma,sans-serif;
  --nav-h:64px;
}
@media (prefers-color-scheme:dark){:root:not([data-theme="light"]){
  --bg:#0A1622; --surface:#112334; --ink:#E8EEF4; --muted:#93A5B5; --line:#223A50;
  --nile:#0A1B2B; --nile-2:#133A5C; --accent:#4FB6C2; --accent-soft:#17384A; --on-nile:#F4EFE3; color-scheme:dark}}
:root[data-theme="dark"]{
  --bg:#0A1622; --surface:#112334; --ink:#E8EEF4; --muted:#93A5B5; --line:#223A50;
  --nile:#0A1B2B; --nile-2:#133A5C; --accent:#4FB6C2; --accent-soft:#17384A; --on-nile:#F4EFE3; color-scheme:dark}
html,body{direction:rtl;text-align:start}
*{box-sizing:border-box}
html,body{margin:0}
body{background:var(--bg);color:var(--ink);font-family:var(--body);font-size:15px;line-height:1.65;
  padding-bottom:calc(var(--nav-h) + env(safe-area-inset-bottom,0px) + 16px)}
.gut{padding-left:max(16px,env(safe-area-inset-left,0px));padding-right:max(16px,env(safe-area-inset-right,0px))}
.col{max-width:760px;margin-inline:auto}

/* header */
.hero{background:linear-gradient(180deg,var(--nile) 0%,var(--nile-2) 100%);color:var(--on-nile);position:relative;overflow:hidden;
  margin-top:calc(-1 * env(safe-area-inset-top,0px));padding-top:calc(env(safe-area-inset-top,0px) + 14px)}
.hero svg{position:absolute;inset:auto 0 0 0;width:100%;height:100%;pointer-events:none}
.hero .in{position:relative;display:flex;justify-content:space-between;align-items:flex-start;gap:12px;padding-bottom:78px}
.brand h1{font-family:var(--display);font-size:1.9rem;font-weight:700;line-height:1.3;margin:0}
.brand p{margin:2px 0 0;font-size:.85rem;opacity:.82}
.refresh{flex:none;display:flex;align-items:center;gap:8px;font:inherit;font-size:.88rem;font-weight:600;color:var(--nile);background:var(--on-nile);
  border:0;border-radius:99px;padding:8px 14px 8px 16px;cursor:pointer}
.refresh svg{position:static;width:18px;height:18px}
.refresh[aria-busy="true"] svg{animation:spin 1s linear infinite}
@keyframes spin{to{transform:rotate(-360deg)}}
.stamp{position:relative;margin:-26px 0 0;font-size:.78rem;color:var(--on-nile);opacity:.9;padding-bottom:10px}

/* feed */
main{padding-block:16px}
.bar{display:flex;justify-content:space-between;align-items:baseline;gap:10px;margin-bottom:10px;flex-wrap:wrap}
.bar h2{font-family:var(--display);font-size:1.25rem;font-weight:700;margin:0}
.bar span{font-size:.82rem;color:var(--muted)}
.chips{display:flex;gap:6px;overflow-x:auto;padding-bottom:8px;margin-bottom:6px;scrollbar-width:thin}
.chip{flex:none;font:inherit;font-size:.82rem;border:1px solid var(--line);background:var(--surface);color:var(--ink);padding:3px 13px;border-radius:99px;cursor:pointer}
.chip[aria-pressed="true"]{background:var(--accent);border-color:var(--accent);color:#fff}
.card{display:block;background:var(--surface);border:1px solid var(--line);border-radius:10px;padding:14px 16px;margin-bottom:12px;color:inherit;text-decoration:none}
.card:hover{border-color:var(--accent)}
.meta{display:flex;gap:8px;align-items:center;flex-wrap:wrap;font-size:.78rem;color:var(--muted)}
.src{font-weight:600;color:var(--accent)}
.tag{border:1px solid var(--line);border-radius:4px;padding:0 6px;font-size:.7rem}
.tag-c{border-color:var(--danger);color:var(--danger)}
.cats{display:flex;gap:6px;margin-bottom:8px}
.hint{font-size:.8rem;color:var(--muted);margin:0 0 10px}
.card h3 a.tl{color:inherit;text-decoration:none}.card h3 a.tl:hover{color:var(--accent)}
.acts{display:flex;gap:14px;align-items:center;flex-wrap:wrap;margin-top:8px}.acts .go{margin-top:0}
.trb{font:inherit;font-size:.8rem;font-weight:600;color:var(--accent);background:var(--accent-soft);border:0;border-radius:99px;padding:3px 12px;cursor:pointer}.trb:disabled{opacity:.6;cursor:default}
.trans{display:flex;flex-direction:column;gap:2px;margin-top:8px;padding:8px 12px;background:var(--accent-soft);border-radius:8px;font-size:.92rem;direction:rtl}.trl{font-size:.72rem;color:var(--muted)}
.card h3{font-size:1.02rem;line-height:1.5;margin:6px 0 4px;font-weight:600;text-wrap:balance}
.card p{margin:0;font-size:.9rem;color:var(--muted)}
.go{display:inline-block;margin-top:8px;font-size:.82rem;color:var(--accent);font-weight:600}
.divider{margin:20px 0 10px;padding-top:12px;border-top:1px dashed var(--line);font-size:.82rem;color:var(--muted)}
.empty{padding:28px 16px;text-align:center;color:var(--muted);border:1px dashed var(--line);border-radius:10px}
.note{font-size:.82rem;color:var(--ink);background:var(--accent-soft);border-radius:8px;padding:6px 12px;margin:0 0 10px}
.state{font-size:.82rem;color:var(--danger);margin:0 0 10px}

/* search view */
.sbox{display:flex;gap:8px;flex-wrap:wrap;margin-bottom:10px}
.sbox input{flex:1 1 220px;min-width:0;font:inherit;padding:11px 14px;border:1px solid var(--line);border-radius:8px;background:var(--surface);color:var(--ink)}
.sbox select{font:inherit;padding:11px 12px;border:1px solid var(--line);border-radius:8px;background:var(--surface);color:var(--ink);max-width:100%}
.shown{font-size:.85rem;color:var(--muted);margin:6px 0 12px;overflow-wrap:anywhere}
.shown b{color:var(--ink)}
.grp{background:var(--surface);border:1px solid var(--line);border-radius:10px;padding:12px 16px;margin-bottom:12px}
.grp h3{font-family:var(--display);font-size:1.02rem;font-weight:700;margin:0 0 4px}
.grp ul{list-style:none;margin:0;padding:0}
.grp li a{display:flex;justify-content:space-between;gap:10px;padding:8px 0;border-top:1px solid var(--line);color:var(--ink);text-decoration:none}
.grp li:first-child a{border-top:0}
.grp li a:hover{color:var(--accent)}
.grp li i{font-style:normal;font-size:.76rem;color:var(--muted)}

/* bottom bar */
nav{position:fixed;inset:auto 0 0 0;z-index:20;background:var(--surface);border-top:1px solid var(--line);
  padding:6px max(10px,env(safe-area-inset-right,0px)) calc(6px + env(safe-area-inset-bottom,0px)) max(10px,env(safe-area-inset-left,0px))}
nav .row{max-width:760px;margin-inline:auto;display:grid;grid-template-columns:repeat(3,1fr);gap:4px}
nav button{position:relative;font:inherit;font-size:.78rem;font-weight:500;color:var(--muted);background:none;border:0;border-radius:10px;padding:8px 4px 6px;cursor:pointer;display:flex;flex-direction:column;align-items:center;gap:2px;min-height:52px}
nav button svg{width:22px;height:22px}
nav button[aria-current="page"]{color:var(--accent);font-weight:600}
nav button[aria-current="page"]::before{content:"";position:absolute;top:-7px;left:30%;right:30%;height:3px;border-radius:0 0 3px 3px;background:var(--gold)}
:focus-visible{outline:2px solid var(--accent);outline-offset:2px}
.mode{position:absolute;top:calc(env(safe-area-inset-top,0px) + 6px);left:max(10px,env(safe-area-inset-left,0px));display:none}
@media (prefers-reduced-motion:reduce){.refresh[aria-busy="true"] svg{animation:none}}
</style>

</head><body><header class="hero gut">
  <svg viewBox="0 0 800 220" preserveAspectRatio="xMidYMax slice" aria-hidden="true" focusable="false">
    <circle cx="610" cy="118" r="44" fill="var(--gold)" opacity=".92"/>
    <rect x="0" y="176" width="800" height="44" fill="var(--nile)" opacity=".55"/>
    <path d="M0 192 Q50 184 100 192 T200 192 T300 192 T400 192 T500 192 T600 192 T700 192 T800 192 V220 H0Z" fill="var(--nile)" opacity=".8"/>
    <g fill="var(--nile)" opacity=".96">
      <path d="M95 176 L112 60 L118 52 L124 60 L141 176Z"/>
      <path d="M95 60 L118 36 L141 60Z" transform="translate(0 16)"/>
      <path d="M205 176 V96 L235 88 V176Z"/><path d="M285 176 V96 L255 88 V176Z"/>
      <rect x="235" y="116" width="20" height="60"/>
      <rect x="318" y="108" width="14" height="68"/><rect x="346" y="108" width="14" height="68"/><rect x="374" y="108" width="14" height="68"/>
      <rect x="310" y="100" width="86" height="10"/>
      <path d="M470 176 C472 140 468 118 476 96 L482 96 C478 120 482 142 484 176Z"/>
      <path d="M479 98 C455 82 440 86 430 96 C448 90 462 92 479 100 C470 78 480 66 492 62 C486 76 486 88 482 98 C500 84 520 84 532 94 C514 90 500 94 482 100Z"/>
      <path d="M700 176 C702 148 698 134 704 118 L709 118 C706 136 710 150 712 176Z"/>
      <path d="M706 120 C690 110 678 112 670 120 C684 116 696 118 706 122 C700 106 708 98 718 96 C712 106 712 114 708 122 C722 112 736 114 744 122 C732 118 720 120 708 124Z"/>
    </g>
  </svg>
  <div class="in col">
    <div class="brand"><h1>رصد الأقصر</h1><p>أخبار المدينة وما يُنشر عنها حول العالم</p></div>
    <button class="refresh" id="refresh" type="button" aria-busy="false">
      <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.2" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><path d="M20 11a8 8 0 1 0-2.3 5.7"/><path d="M20 4v7h-7"/></svg>
      <span>تحديث</span>
    </button>
  </div>
  <div class="col"><p class="stamp" id="stamp">جار التحميل</p></div>
</header>

<main class="gut"><div class="col" id="view"></div></main>

<nav aria-label="التنقل الرئيسي"><div class="row">
  <button type="button" data-tab="local" aria-current="page">
    <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><path d="M4 20V10l8-6 8 6v10"/><path d="M9 20v-6h6v6"/></svg>أخبار محلية</button>
  <button type="button" data-tab="world">
    <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><circle cx="12" cy="12" r="9"/><path d="M3 12h18M12 3c3 3 3 15 0 18M12 3c-3 3-3 15 0 18"/></svg>أخبار عالمية</button>
  <button type="button" data-tab="search">
    <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><circle cx="11" cy="11" r="7"/><path d="M20 20l-4-4"/></svg>بحث</button>
</div></nav>

<script>
document.documentElement.setAttribute("dir","rtl");document.documentElement.setAttribute("lang","ar");
/* Fallback feed, used when the shared database is empty or unreachable. date:null means the source page gave no verifiable date. */
var SEED=[];
var LN={ar:'العربية',en:'English',fr:'Français',de:'Deutsch',es:'Español',it:'Italiano',ru:'Русский',tr:'Türkçe',zh:'中文',ja:'日本語'};
var LQ={ar:{t:'الأقصر',hl:'ar',ceid:'EG:ar'},en:{t:'Luxor Egypt',hl:'en',ceid:'EG:en'},fr:{t:'Louxor Égypte',hl:'fr',ceid:'FR:fr'},de:{t:'Luxor Ägypten',hl:'de',ceid:'DE:de'},es:{t:'Luxor Egipto',hl:'es',ceid:'ES:es'},it:{t:'Luxor Egitto',hl:'it',ceid:'IT:it'},ru:{t:'Луксор Египет',hl:'ru',ceid:'RU:ru'},tr:{t:'Luksor Mısır',hl:'tr',ceid:'TR:tr'},zh:{t:'卢克索 埃及',hl:'zh-CN',ceid:'CN:zh-Hans'},ja:{t:'ルクソール エジプト',hl:'ja',ceid:'JP:ja'}};
var E=encodeURIComponent;
var ST={note:'',tab:'local',news:SEED.slice(),wl:'all',ls:'all',cat:'all',tr:{},trShow:{},trFail:{},trBusy:{},sl:'ar',sq:'',loadedAt:null,from:'seed',err:''};
var view=document.getElementById('view');

function ts(n){return n.date?Date.parse(n.date):NaN}
function sorted(list){
  var dated=list.filter(function(n){return !isNaN(ts(n))}).sort(function(a,b){return ts(b)-ts(a)});
  var undated=list.filter(function(n){return isNaN(ts(n))});
  return {dated:dated,undated:undated};
}
function fmt(n){
  var d=new Date(ts(n));var now=new Date();
  var day=function(x){return new Date(x.getFullYear(),x.getMonth(),x.getDate()).getTime()};
  var dd=Math.round((day(now)-day(d))/86400000);
  if(n.dp==='day'){
    if(dd===0)return 'اليوم';if(dd===1)return 'أمس';
    return d.toLocaleDateString('ar-EG',{day:'numeric',month:'long',year:'numeric'});
  }
  var diff=(now.getTime()-d.getTime())/3600000;
  if(diff>=0&&diff<1)return 'قبل قليل';
  if(diff>=1&&diff<24)return 'منذ '+Math.floor(diff)+' ساعة';
  if(dd===1)return 'أمس';
  return d.toLocaleDateString('ar-EG',{day:'numeric',month:'long',year:'numeric'});
}
function el(tag,cls,txt){var e=document.createElement(tag);if(cls)e.className=cls;if(txt!=null)e.textContent=txt;return e}

function trUrl(n){return 'https://translate.google.com/translate?sl=auto&tl=ar&u='+E(n.url)}
async function doTr(n){window.open(trUrl(n),'_blank','noopener');return;
  if(ST.tr[n.id]){ST.trShow[n.id]=!ST.trShow[n.id];render();return}
  if(ST.trBusy[n.id])return;
  ST.trBusy[n.id]=true;ST.trFail[n.id]=false;render();
  try{
    var sm=window.claude&&claude.use?await claude.use('sample'):null;
    if(!sm)throw {code:'none'};
    var r=await sm('ترجم الخبر التالي إلى العربية الفصحى. أخرج سطرين فقط بهذا الشكل بلا أي إضافة:\nالعنوان: <العنوان المترجم>\nالملخص: <الملخص المترجم، أو اتركه فارغا إن لم يوجد>\n\nالعنوان: '+n.title+'\nالملخص: '+(n.summary||''),{modelTier:'quick'});
    var t=(r&&r.text)||'';var mt=t.match(/العنوان:\s*(.+)/),ms=t.match(/الملخص:\s*(.*)/);
    ST.tr[n.id]={t:(mt?mt[1]:t).trim(),s:ms?ms[1].trim():''};ST.trShow[n.id]=true;
  }catch(e){ST.trFail[n.id]=true}
  ST.trBusy[n.id]=false;render();
}
function card(n){
  var d=el('article','card');
  var m=el('div','meta');m.appendChild(el('span','src',n.source));
  if(n.lang!=='ar')m.appendChild(el('span','tag',LN[n.lang]||n.lang));
  (n.tags||[]).forEach(function(t){m.appendChild(el('span','tag tag-c',t))});
  m.appendChild(el('span',null,isNaN(ts(n))?'التاريخ غير متاح':fmt(n)));
  d.appendChild(m);
  var h=el('h3');var ha=el('a','tl',n.title);ha.href=n.url;ha.target='_blank';ha.rel='noopener noreferrer';h.appendChild(ha);if(n.lang!=='ar'){h.dir='auto'}d.appendChild(h);
  if(n.summary){var p=el('p',null,n.summary);if(n.lang!=='ar')p.dir='auto';d.appendChild(p)}
  var tr=ST.tr[n.id];
  if(tr&&ST.trShow[n.id]){var tb=el('div','trans');tb.appendChild(el('span','trl','ترجمة آلية إلى العربية'));tb.appendChild(el('strong',null,tr.t));if(tr.s)tb.appendChild(el('span',null,tr.s));d.appendChild(tb)}
  var acts=el('div','acts');
  var go=el('a','go','قراءة الخبر ←');go.href=n.url;go.target='_blank';go.rel='noopener noreferrer';acts.appendChild(go);
  if(n.lang!=='ar'){
    var b=el('button','trb',ST.trBusy[n.id]?'جار الترجمة...':(tr?(ST.trShow[n.id]?'إخفاء الترجمة':'عرض الترجمة'):'ترجمة إلى العربية'));
    b.type='button';b.disabled=!!ST.trBusy[n.id];b.onclick=function(){doTr(n)};acts.appendChild(b);
    if(ST.trFail[n.id]){var g=el('a','go','ترجمة عبر جوجل');g.href=trUrl(n);g.target='_blank';g.rel='noopener noreferrer';acts.appendChild(g);d.appendChild(acts);d.appendChild(el('p','state','تعذرت الترجمة داخل الصفحة. يمكنك فتح الصفحة المترجمة عبر جوجل.'));return d}
  }
  d.appendChild(acts);
  return d;
}
async function trAll(){
  var todo=ST.news.filter(function(n){return n.lang!=='ar'&&!ST.tr[n.id]});
  for(var i=0;i<todo.length;i++){await doTr(todo[i]);if(ST.trFail[todo[i].id])break}
}
function feed(isLocal){
  var list=ST.news.filter(function(n){return isLocal?n.lang==='ar':n.lang!=='ar'});
  var bar=el('div','bar');bar.appendChild(el('h2',null,isLocal?'الأخبار المحلية':'الأخبار العالمية'));
  bar.appendChild(el('span',null,'الأحدث أولا'));
  if(false){var ta=el('button','trb','ترجمة الكل إلى العربية');ta.type='button';ta.onclick=trAll;bar.appendChild(ta)}
  view.appendChild(bar);
  if(isLocal){
    var cc=el('div','cats');
    [['all','كل الأخبار'],['social','من مواقع التواصل'],['complaint','شكاوى واستغاثات']].forEach(function(x){var c=el('button','chip',x[1]);c.type='button';c.setAttribute('aria-pressed',String(ST.cat===x[0]));c.onclick=function(){ST.cat=x[0];render()};cc.appendChild(c)});
    view.appendChild(cc);
    if(ST.cat==='social'){list=list.filter(function(n){return n.social});view.appendChild(el('p','hint','أخبار الصحف عن منشورات وفيديوهات انتشرت على مواقع التواصل، مع توضيحات الجهات الرسمية حيث وُجدت.'))}
    if(ST.cat==='complaint'){list=list.filter(function(n){return n.cat==='complaint'});view.appendChild(el('p','hint','تعرض هذه الصفحة ما نشرته الصحف عن الشكاوى والفيديوهات المتداولة على مواقع التواصل، ولا تنقل منشورات الأفراد مباشرة. الأسماء والعناوين الخاصة محذوفة.'))}
  }
  var key=isLocal?'source':'lang',cur=isLocal?ST.ls:ST.wl;
  var vals=['all'];list.forEach(function(n){if(vals.indexOf(n[key])<0)vals.push(n[key])});
  if(vals.length>2){
    var cw=el('div','chips');
    vals.forEach(function(v){var c=el('button','chip',v==='all'?(isLocal?'كل المصادر':'كل اللغات'):(isLocal?v:(LN[v]||v)));c.type='button';c.setAttribute('aria-pressed',String(cur===v));
      c.onclick=function(){if(isLocal)ST.ls=v;else ST.wl=v;render()};cw.appendChild(c)});
    view.appendChild(cw);
    if(cur!=='all')list=list.filter(function(n){return n[key]===cur});
  }
  if(ST.err)view.appendChild(el('p','state',ST.err));
  if(ST.note)view.appendChild(el('p','note',ST.note));
  var s=sorted(list);
  if(!list.length){view.appendChild(el('div','empty','لا توجد أخبار في هذا القسم بعد.'));return}
  s.dated.forEach(function(n){view.appendChild(card(n))});
  if(s.undated.length){view.appendChild(el('div','divider','أخبار لم يظهر تاريخ نشرها في المصدر، وتُعرض بعد الأخبار المؤرخة'));
    s.undated.forEach(function(n){view.appendChild(card(n))})}
}

function gnews(q,l,site){return 'https://news.google.com/search?q='+E(q+(site?' site:'+site:''))+'&hl='+l.hl+'&gl='+l.ceid.split(':')[0]+'&ceid='+l.ceid}
function searchView(){
  var bar=el('div','bar');bar.appendChild(el('h2',null,'ابحث عن خبر'));bar.appendChild(el('span',null,'يفتح النتائج في المصدر'));view.appendChild(bar);
  var f=el('form','sbox');
  var i=el('input');i.type='search';i.id='sq';i.placeholder='مثال: معبد الكرنك، حادث، افتتاح';i.value=ST.sq;i.setAttribute('aria-label','كلمة البحث');
  var s=el('select');s.id='sl';s.setAttribute('aria-label','لغة البحث');
  Object.keys(LN).forEach(function(k){var o=el('option',null,LN[k]);o.value=k;if(k===ST.sl)o.selected=true;s.appendChild(o)});
  f.appendChild(i);f.appendChild(s);view.appendChild(f);
  var shown=el('p','shown');view.appendChild(shown);
  var out=el('div');view.appendChild(out);
  function draw(){
    var l=LQ[ST.sl];var q=(ST.sq.trim()?ST.sq.trim()+' ':'')+l.t; if(ST.sq.trim()&&ST.sq.indexOf(l.t.split(' ')[0])>-1)q=ST.sq.trim();
    shown.textContent='';shown.appendChild(document.createTextNode('عبارة البحث: '));shown.appendChild(el('b',null,q));
    out.textContent='';
    function S(arr){return arr.map(function(x){return [x[0],gnews(q,l,x[1]),x[1]]})}
    var G=[
     ['محركات الأخبار',[['أخبار جوجل',gnews(q,l)],['بحث جوجل','https://www.google.com/search?q='+E(q)+'&hl='+l.hl],['أخبار Bing','https://www.bing.com/news/search?q='+E(q)],['أخبار Yahoo','https://news.search.yahoo.com/search?p='+E(q)],['DuckDuckGo','https://duckduckgo.com/?q='+E(q)+'&iar=news&ia=news'],['اتجاهات جوجل','https://trends.google.com/trends/explore?q='+E(q)+'&geo=EG']]],
     ['شبكات رصد الإخبارية',(function(){var qa=(ST.sq.trim()?ST.sq.trim()+' ':'')+'الأقصر';var out=[];['شبكة رصد','رصد المحافظات','موقع رصد الإخباري'].forEach(function(nm){var t='"'+nm+'" '+qa;out.push([nm+': أخبار جوجل',gnews(t,LQ.ar)],[nm+': صفحات فيسبوك','https://www.facebook.com/search/pages?q='+E(nm)],[nm+': منشورات فيسبوك','https://www.facebook.com/search/posts?q='+E(t)],[nm+': X','https://x.com/search?q='+E(t)+'&f=live'])});out.unshift(['صفحة فيسبوك خامسة أضفتها (رصد)','https://www.facebook.com/share/14vTNDpPdxK/','facebook.com']);out.unshift(['صفحة فيسبوك رابعة أضفتها (رصد)','https://www.facebook.com/share/1CgEPbbXkv/','facebook.com']);out.unshift(['صفحة فيسبوك ثالثة أضفتها (رصد)','https://www.facebook.com/share/1GD1qYh8hQ/','facebook.com']);out.unshift(['صفحة فيسبوك ثانية أضفتها (رصد)','https://www.facebook.com/share/1EPnymgy5L/','facebook.com']);out.unshift(['صفحة فيسبوك التي أضفتها (رصد)','https://www.facebook.com/share/1KzukKZpYx/','facebook.com']);out.push(['تنبيهات جوجل: متابعة مستمرة','https://www.google.com/alerts?q='+E(qa),'google.com/alerts']);return out})()],
     ['منصات التواصل',[['X','https://x.com/search?q='+E(q)+'&f=live'],['فيسبوك','https://www.facebook.com/search/posts?q='+E(q)],['إنستجرام','https://www.instagram.com/explore/search/keyword/?q='+E(q)],['يوتيوب','https://www.youtube.com/results?search_query='+E(q)],['تيك توك','https://www.tiktok.com/search?q='+E(q)],['ثريدز','https://www.threads.net/search?q='+E(q)],['Bluesky','https://bsky.app/search?q='+E(q)],['ريديت','https://www.reddit.com/search/?q='+E(q)+'&sort=new'],['لينكدإن','https://www.linkedin.com/search/results/content/?keywords='+E(q)],['بنترست','https://www.pinterest.com/search/pins/?q='+E(q)],['تيليجرام',gnews(q,l,'t.me'),'t.me'],['فيسبوك: فيديوهات','https://www.facebook.com/search/videos?q='+E(q)],['X: صور وفيديو','https://x.com/search?q='+E(q)+'&f=media'],['ريديت r/Egypt','https://www.reddit.com/r/Egypt/search/?q='+E(q)+'&restrict_sr=1&sort=new'],['كورا','https://www.quora.com/search?q='+E(q)],['تامبلر','https://www.tumblr.com/search/'+E(q)],['فليكر','https://www.flickr.com/search/?text='+E(q)+'&sort=date-posted-desc'],['فيميو','https://vimeo.com/search?q='+E(q)],['ديلي موشن','https://www.dailymotion.com/search/'+E(q)],['تريب أدفايزر','https://www.tripadvisor.com/Search?q='+E(q)],['فيسبوك: مجموعات','https://www.facebook.com/search/groups?q='+E(q)],['فيسبوك: صفحات','https://www.facebook.com/search/pages?q='+E(q)],['صور جوجل','https://www.google.com/search?tbm=isch&q='+E(q)],['فيديوهات جوجل','https://www.google.com/search?tbm=vid&q='+E(q)],['خرائط جوجل: تقييمات','https://www.google.com/maps/search/'+E(q)],['ميديوم','https://medium.com/search?q='+E(q)],['سابستاك','https://substack.com/search/'+E(q)],['تويتش','https://www.twitch.tv/search?term='+E(q)],['VK (روسيا)','https://vk.com/search?c%5Bq%5D='+E(q)+'&c%5Bsection%5D=auto'],['ويبو (الصين)','https://s.weibo.com/weibo?q='+E(q)],['شياوهونغشو (الصين)','https://www.xiaohongshu.com/search_result?keyword='+E(q)],['دويين (الصين)','https://www.douyin.com/search/'+E(q)]]],
     ['حسابات رسمية على المنصات',[['فيسبوك: صفحة محافظة الأقصر (1)','https://www.facebook.com/Luxor.Governorate.Official/','facebook.com'],['فيسبوك: صفحة محافظة الأقصر (2)','https://www.facebook.com/governorate.luxor/','facebook.com'],['البوابة الرسمية لمحافظة الأقصر','http://www.luxor.gov.eg/default.aspx','luxor.gov.eg'],['تيك توك: صفحة الأقصر','https://www.tiktok.com/travel/Luxor-22535865246695123','tiktok.com']]],
     ['شكاوى واستغاثات على المنصات',[['فيسبوك: استغاثة','https://www.facebook.com/search/posts?q='+E('استغاثة الأقصر')],['فيسبوك: شكوى','https://www.facebook.com/search/posts?q='+E('شكوى الأقصر')],['X: استغاثة','https://x.com/search?q='+E('استغاثة الأقصر')+'&f=live'],['X: شكوى','https://x.com/search?q='+E('شكوى الأقصر محافظ')+'&f=live'],['يوتيوب: فيديو استغاثة','https://www.youtube.com/results?search_query='+E('استغاثة الأقصر')],['تيك توك: استغاثة','https://www.tiktok.com/search?q='+E('استغاثة الأقصر')],['إنستجرام: وسم','https://www.instagram.com/explore/tags/'+E('الأقصر')+'/'],['أخبار جوجل: فيديو متداول',gnews('فيديو متداول الأقصر',LQ.ar)],['اليوم المفتوح بمحافظة الأقصر',gnews('اليوم المفتوح محافظ الأقصر شكاوى',LQ.ar)],['منظومة الشكاوى الحكومية الموحدة','https://www.google.com/search?q='+E('منظومة الشكاوى الحكومية الموحدة تقديم شكوى')+'&hl=ar']]],
     ['الصحف والمواقع المصرية',S([['اليوم السابع','youm7.com'],['المصري اليوم','almasryalyoum.com'],['الأهرام','ahram.org.eg'],['مصراوي','masrawy.com'],['الشروق','shorouknews.com'],['الوطن','elwatannews.com'],['القاهرة 24','cairo24.com'],['صدى البلد','elbalad.news'],['الفجر','elfagr.org'],['الدستور','dostor.org'],['فيتو','vetogate.com'],['البوابة نيوز','albawabhnews.com'],['أخبار اليوم','akhbarelyom.com'],['الوفد','alwafd.news'],['المشهد','almshhdnews.com']])],
     ['الصحف المصرية بالإنجليزية',S([['Ahram Online','english.ahram.org.eg'],['Egypt Independent','egyptindependent.com'],['Egypt Today','egypttoday.com'],['Daily News Egypt','dailynewsegypt.com'],['Egyptian Gazette','egyptian-gazette.com'],['Egyptian Streets','egyptianstreets.com']])],
     ['الأخبار الإقليمية والعالمية',S([['BBC','bbc.com'],['CNN','cnn.com'],['Reuters','reuters.com'],['Associated Press','apnews.com'],['الجزيرة','aljazeera.net'],['العربية','alarabiya.net'],['France 24','france24.com'],['DW','dw.com'],['Euronews','euronews.com'],['The Guardian','theguardian.com'],['New York Times','nytimes.com'],['الشرق الأوسط','aawsat.com'],['The National','thenationalnews.com'],['Arab News','arabnews.com'],['Times of Israel','timesofisrael.com']])],
     ['السياحة والآثار والمجلات',S([['National Geographic','nationalgeographic.com'],['Archaeology Magazine','archaeology.org'],['Smithsonian','smithsonianmag.com'],['Live Science','livescience.com'],['Arkeonews','arkeonews.net'],['Travel And Tour World','travelandtourworld.com'],['TravelDailyNews','traveldailynews.com'],['Travel + Leisure','travelandleisure.com'],['Lonely Planet','lonelyplanet.com'],['Condé Nast Traveler','cntraveler.com']])],
     ['جهات رسمية',S([['محافظة الأقصر','luxor.gov.eg'],['المجلس الأعلى للآثار','egymonuments.gov.eg'],['الهيئة العامة للاستعلامات','sis.gov.eg'],['مجلس الوزراء','cabinet.gov.eg']])]
    ];
    G.forEach(function(g){var box=el('div','grp');box.appendChild(el('h3',null,g[0]));var ul=el('ul');
      g[1].forEach(function(it){var li=el('li'),a=el('a');a.href=it[1];a.target='_blank';a.rel='noopener noreferrer';a.appendChild(el('span',null,it[0]));var t=el('i',null,it[2]||'فتح ←');t.dir='ltr';a.appendChild(t);li.appendChild(a);ul.appendChild(li)});
      box.appendChild(ul);out.appendChild(box)});
  }
  i.oninput=function(){ST.sq=i.value;draw()};
  s.onchange=function(){ST.sl=s.value;draw()};
  f.onsubmit=function(e){e.preventDefault();draw()};
  draw();
}

function render(){
  view.textContent='';
  document.querySelectorAll('nav button').forEach(function(b){if(b.dataset.tab===ST.tab)b.setAttribute('aria-current','page');else b.removeAttribute('aria-current')});
  if(ST.tab==='local')feed(true);else if(ST.tab==='world')feed(false);else searchView();
  var st=document.getElementById('stamp');
  st.textContent=ST.loadedAt?('آخر تحديث '+ST.loadedAt.toLocaleTimeString('ar-EG',{hour:'numeric',minute:'2-digit'})+(ST.from==='db'?' من الإنترنت':' من الأخبار المحفوظة')):'جار التحميل';
}

var busy=false,known=null;
function norm(r){return {id:r.id,lang:r.lang||'ar',source:r.source||'',title:r.title,summary:r.summary||'',date:r.date||null,dp:'time',cat:r.cat||'',social:!!r.social,tags:r.tags||[],url:r.url}}
async function pull(url,opt){var r=await fetch(url,opt);if(!r.ok)throw new Error(r.status);return r.json()}
function take(j){
  var rows=(j.news||[]).map(norm),ids={},fresh=0;
  rows.forEach(function(r){ids[r.id]=1;if(known&&!known[r.id])fresh++});
  if(j.status&&j.status.version){if(ST.ver&&ST.ver!==j.status.version){location.reload();return 0}ST.ver=j.status.version}
  known=ids;ST.news=rows;ST.from='db';ST.status=j.status||{};return fresh;
}
async function load(manual){
  if(busy)return;busy=true;
  var b=document.getElementById('refresh');b.setAttribute('aria-busy','true');ST.err='';
  try{
    var j,total=0;
    if(manual){ST.note='بدأ الجمع من المصادر...';render();j=await pull('/api/refresh',{method:'POST'})}
    else j=await pull('/api/news');
    total+=take(j);
    var guard=0;
    while(j.status&&j.status.running&&guard<300){
      ST.note='جار الجمع: '+(j.status.done||0)+' من '+(j.status.sources||'...')+' مصدرا، والأخبار تظهر أولا بأول';
      ST.loadedAt=new Date();render();
      await new Promise(function(r){setTimeout(r,2000)});
      j=await pull('/api/news');total+=take(j);guard++;
    }
    if(manual||guard>0)ST.note=total>0?('تمت إضافة '+total+' خبر جديد من الإنترنت'):'لا توجد أخبار جديدة منذ آخر تحديث';
    else ST.note='';
  }catch(e){ST.err='تعذر الاتصال ببرنامج رصد الأقصر. تأكد أنه ما زال يعمل.';ST.note=''}
  ST.loadedAt=new Date();busy=false;b.setAttribute('aria-busy','false');render();
}
setInterval(async function(){
  if(busy)return;
  try{var j=await pull('/api/news');var f=take(j);
    if(f>0){ST.note='وصلت '+f+' أخبار جديدة';ST.loadedAt=new Date();render()}
    else if(!(j.status&&j.status.running)&&ST.note.indexOf('جار')===0){ST.note='';render()}
    else if(ST.news.length&&ST.from==='db'&&document.getElementById('view').children.length<3){render()}
  }catch(e){}
},20000);
document.getElementById('refresh').onclick=function(){load(true)};
document.querySelectorAll('nav button').forEach(function(b){b.onclick=function(){ST.tab=b.dataset.tab;render();window.scrollTo(0,0)}});
render();load(false);if("serviceWorker" in navigator)navigator.serviceWorker.register("/sw.js").catch(function(){});
</script>
</body></html>'''

TEST_FEED = """<?xml version="1.0"?><rss><channel>
<item><title>حريق محدود في وسط مدينة الأقصر - اليوم السابع</title><link>https://example.com/1</link><pubDate>Sun, 22 Nov 2026 08:51:00 GMT</pubDate><source>اليوم السابع</source><description>x</description></item>
<item><title>فيديو متداول يثير غضب أهالي الأقصر</title><link>https://example.com/2</link><pubDate>Sun, 22 Nov 2026 07:00:00 GMT</pubDate></item>
<item><title>New tomb found near Luxor, Egypt</title><link>https://example.com/3</link><pubDate>Sat, 21 Nov 2026 10:00:00 GMT</pubDate><source>Ahram Online</source></item>
<item><title>Luxor Las Vegas hotel opens new pool</title><link>https://example.com/4</link><pubDate>Sat, 21 Nov 2026 10:00:00 GMT</pubDate></item>
<item><title>خبر عن القاهرة فقط</title><link>https://example.com/5</link><pubDate>Sat, 21 Nov 2026 10:00:00 GMT</pubDate></item>
</channel></rss>"""

if __name__ == "__main__":
    load_db()
    if "--update-url" in sys.argv:
        k = sys.argv.index("--update-url")
        open(UPDATE_FILE, "w", encoding="utf-8").write(sys.argv[k + 1].strip())
        log("تم حفظ رابط التحديث. شغّل البرنامج من جديد.")
        sys.exit(0)
    if "--publish" in sys.argv:
        # وضع GitHub Pages: يقرأ news.json السابق ويضيف الجديد ثم يحفظه
        pub = os.path.join(HERE, "news.json")
        try:
            old = json.load(open(pub, encoding="utf-8")).get("news", [])
            STATE["items"] = {n["id"]: n for n in old if n.get("id")}
        except Exception:
            pass
        refresh_once()
        snap = snapshot()
        with open(pub + ".tmp", "w", encoding="utf-8") as f:
            json.dump(snap, f, ensure_ascii=False)
        os.replace(pub + ".tmp", pub)
        log("تم حفظ news.json:", len(snap["news"]), "خبرا")
        sys.exit(0)
    if "--once" in sys.argv:
        refresh_once()
        export_files()
        log("تم حفظ luxor_news.json و luxor_news.csv")
        sys.exit(0)
    threading.Thread(target=background, daemon=True).start()
    srv = Quiet((HOST, PORT), H)
    if HOST == "0.0.0.0":
        try:
            sk = socket.socket(socket.AF_INET, socket.SOCK_DGRAM); sk.connect(("8.8.8.8", 80)); ip = sk.getsockname()[0]
        except Exception:
            ip = "عنوان-الجهاز"
        log("من الهاتف (نفس الواي فاي) افتح: http://%s:%d" % (ip, PORT))
    log("افتح http://localhost:%d  (الجمع الأول يستغرق نحو دقيقة)" % PORT)
    try:
        srv.serve_forever()
    except KeyboardInterrupt:
        pass
