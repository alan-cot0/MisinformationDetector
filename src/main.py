from RealtimeSTT import AudioToTextRecorder
import signal, sys

SENTINEL = "Alexa, stop."

class boolPtr:
    def __init__(this, boolean):
        this.boolean = boolean
    def set(this, boolean):
        this.boolean = boolean
    def get(this):
        return this.boolean

def process_text(text):
    print(text)
    if(str(text) == SENTINEL):
        signal.raise_signal(signal.SIGINT)

# This is the test example. I'd need to figure out how to implement a sentinel check.
if __name__ == '__main__':
    def handle_interrupt(_signal, frame=None):
        if(_signal == signal.SIGINT):
            continuing.set(False)
            recorder.shutdown()
            # print("\nConfirm stopping the program? (Say Yes or No.)")

    continuing = boolPtr(True)
    signal.signal(signal.SIGINT, handle_interrupt)
    print("Wait until it says 'speak now'")
    recorder = AudioToTextRecorder()

    while True:
        recorder.text(process_text)
        if(not continuing.get()):
            break
    sys.stdin.flush()
    print("\nExiting...")
    # recorder.shutdown()