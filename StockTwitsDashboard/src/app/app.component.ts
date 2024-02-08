import { Component, OnDestroy, OnInit } from '@angular/core';
import { RouterOutlet } from '@angular/router';
import { ConnectionStatus, QuixService } from './services/quix.service';
import { Subject, filter, take, takeUntil } from 'rxjs';
import { CommonModule } from '@angular/common';
import { MessagePayload } from './models/messagePayload';
import { ParameterData } from './models/parameterPayload';
import { MaterialModule } from './material.module';


export const POSITIVE_THRESHOLD = 0.5;
export const NEGATIVE_THRESHOLD = -0.5;

export interface Message {
  timestamp: Date;
  message: string;
  sentiment: { label: string, score: number }
}

@Component({
  selector: 'app-root',
  standalone: true,
  imports: [RouterOutlet, CommonModule, MaterialModule],
  templateUrl: './app.component.html',
  styleUrl: './app.component.scss',
})
export class AppComponent implements OnInit, OnDestroy {
  title = 'stockTwitsDashboard';
  connectionState = ConnectionStatus;
  readerConnectionStatus: ConnectionStatus = ConnectionStatus.Offline;
  writerConnectionStatus: ConnectionStatus = ConnectionStatus.Offline;

  messages: Message[] = [];

  private unsubscribe$ = new Subject<void>();

  constructor(private quixService: QuixService) {}

  ngOnInit(): void {
    // Listen for connection status changes
    this.quixService.readerConnStatusChanged$
      .pipe(takeUntil(this.unsubscribe$))
      .subscribe((status) => {
        this.readerConnectionStatus = status;
      });
    this.quixService.writerConnStatusChanged$
      .pipe(takeUntil(this.unsubscribe$))
      .subscribe((status) => {
        this.writerConnectionStatus = status;
      });

    this.quixService.readerConnStatusChanged$.pipe(filter((f) => f === this.connectionState.Connected), take(1)).subscribe(() => {
      this.quixService.subscribeToParameter('messages4', 'message', '*');
      this.quixService.subscribeToParameter('messages-with-sentiment', 'CSV_DATA_083', '*');
    })

    // Listen for reader messages
    this.quixService.paramDataReceived$
      .pipe(
        takeUntil(this.unsubscribe$),
        //filter((f) => f.streamId === this.roomService.selectedRoom) // Ensure there is no message leaks
      )
      .subscribe((payload) => {
        console.log('PAYLOAD RECIEVED', payload);
        this.messageReceived(payload);
      });
  }

  messageReceived(payload: ParameterData): void {
    const { topicName } = payload;
    const timestamp = payload.timestamps?.at(0)!;
    const label = payload.stringValues['label']?.at(0)!;
    const text = payload.stringValues['text']?.at(0)!;
    const score = payload.numericValues['score']?.at(0)!;

    const newMessage: Message = {
      message: text,
      sentiment: {
        label,
        score
      },
      timestamp: new Date(timestamp)
    }

    console.log('Adding new mssag', newMessage);
    this.messages.push(newMessage);
  }

  ngOnDestroy(): void {
    this.unsubscribe$.next();
    this.unsubscribe$.complete();
  }
}
