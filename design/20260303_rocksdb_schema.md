Change the schema of the RocksDB to the one below. If multiple entries are found for zobr64 then all entries should be present. 
What to do with values that are out of the bounds for possible values:
- If the score is more than max i16 then use the max. 
- If the score is less than min i16 then use the min. 
- If the depth is more than 255 then use 255.

Use zstd compression of level 3 for normal level and level 6 for bottommost.

Run a full CompactRange (per CF) so you end up with data mostly in bottommost level, maximally compressed, and with fewer redundant versions.

## Eval RocksDB schema

### Key

* `k = be_u64(zobr64)` — **8 bytes** (big-endian u64)

---

### Value

```
u8 count                  // 2..18
repeat count times:
    i16 score             // signed
    u8  depth             // 0..255
    u16 move_meta         // packed (includes score_kind + promo)
    u8[34] board34        // fixed 34-byte board encoding
```

### Sizes

* Per entry: `2 + 1 + 2 + 34 = 39 bytes`
* Total value size: `1 + 39 * count` bytes
  (count=2 → 79 bytes, count=18 → 703 bytes)

### Endianness (recommended)

* `score`: little-endian `i16`
* `move_meta`: little-endian `u16`
* `board34`: raw byte array
* Key: big-endian `u64` (as specified)

---

## Decoding

### Extract fields from `move_meta`

```
from  =  move_meta        & 0x3F
to    = (move_meta >> 6)  & 0x3F
promo = (move_meta >> 12) & 0x07
kind  = (move_meta >> 15) & 0x01   // 0 eval, 1 mate
```

### Score semantics

* `kind == 0` → `score` is **centipawns**
* `kind == 1` → `score` is **mate in moves** (signed)

---

# board34 format (34-byte board encoding)

`board34` encodes a chess position (piece placement + side to move + castling + en-passant), excluding halfmove/fullmove counters.

Total: **34 bytes**

```
Bytes 0–31 : 64 squares packed as 4-bit nibbles (2 squares per byte)
Bytes 32–33: position state (side, castling, en-passant)
```

---

## 1) Piece encoding (Bytes 0–31)

Each square is **4 bits** (a nibble). Two squares per byte:

* `byte[i] low  nibble` = square `(2*i)`
* `byte[i] high nibble` = square `(2*i + 1)`

### Square indexing

Use a stable ordering (recommended):

* `a1 = 0, b1 = 1, …, h1 = 7, a2 = 8, …, h8 = 63` (rank-major from White’s side)

### Piece codes (4-bit)

```
0  = empty

1  = white pawn
2  = white knight
3  = white bishop
4  = white rook
5  = white queen
6  = white king

7  = black pawn
8  = black knight
9  = black bishop
10 = black rook
11 = black queen
12 = black king

13–15 reserved
```

---

## 2) Position state (Bytes 32–33)

### Byte 32: side + castling

```
bit 0 : side to move (0 = white, 1 = black)
bit 1 : white can castle king-side
bit 2 : white can castle queen-side
bit 3 : black can castle king-side
bit 4 : black can castle queen-side
bit 5–7 : reserved (set to 0)
```

### Byte 33: en-passant

```
0–63  = en-passant target square index (same 0..63 mapping as above)
255   = no en-passant
```
