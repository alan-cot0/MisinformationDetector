# main.py
import asyncio
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from RealtimeSTT import AudioToTextProcessor  #  adapted STT class
from fastapi import FastAPI, UploadFile, File
from gemini_TRUTH_ANALYZER import gemini_fact_check

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.websocket("/ws/audio")
async def websocket_audio_endpoint(websocket: WebSocket):
    await websocket.accept()
    print("Client connected")

    # Create an instance of the STT processor
    stt_processor = AudioToTextProcessor()  

    try:
        while True:
            audio_chunk = await websocket.receive_bytes()
            
            # Convert audio chunk -> text
            text = stt_processor.process_chunk(audio_chunk)

            # Run Gemini truth analysis
            #truth_score, message = analyze_truth(text)
            @app.post("/truth-check/")
            async def truth_check(text: str, wiki_paragraphs: list[str]):
                verdict = gemini_fact_check(text, wiki_paragraphs)
                return {"verdict": verdict}
            truth_score = truth_check(text)

            await websocket.send_json({
                "transcript": text,
                "truth_score": truth_score,
                #"message": message
            })

    except WebSocketDisconnect:
        print("Client disconnected")
        stt_processor.shutdown()
